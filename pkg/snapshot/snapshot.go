/*
Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package snapshot

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"syscall"

	"github.com/GoogleContainerTools/kaniko/pkg/timing"

	"github.com/karrick/godirwalk"

	"github.com/GoogleContainerTools/kaniko/pkg/constants"

	"github.com/GoogleContainerTools/kaniko/pkg/util"
	"github.com/sirupsen/logrus"
)

// For testing
var snapshotPathPrefix = constants.KanikoDir

// Snapshotter holds the root directory from which to take snapshots, and a list of snapshots taken
type Snapshotter struct {
	l         *LayeredMap
	directory string
}

// NewSnapshotter creates a new snapshotter rooted at d
func NewSnapshotter(l *LayeredMap, d string) *Snapshotter {
	return &Snapshotter{l: l, directory: d}
}

// Init initializes a new snapshotter
func (s *Snapshotter) Init() error {
	_, _, err := s.scanFullFilesystem()
	return err
}

// Key returns a string based on the current state of the file system
func (s *Snapshotter) Key() (string, error) {
	return s.l.Key()
}

// TakeSnapshot takes a snapshot of the specified files, avoiding directories in the whitelist, and creates
// a tarball of the changed files. Return contents of the tarball, and whether or not any files were changed
func (s *Snapshotter) TakeSnapshot(files []string) (string, error) {
	f, err := ioutil.TempFile(snapshotPathPrefix, "")
	if err != nil {
		return "", err
	}
	defer f.Close()

	s.l.Snapshot()
	if len(files) == 0 {
		logrus.Info("No files changed in this command, skipping snapshotting.")
		return "", nil
	}
	logrus.Info("Taking snapshot of files...")
	logrus.Debugf("Taking snapshot of files %v", files)

	// Also add parent directories to keep the permission of them correctly.
	filesToAdd := filesWithParentDirs(files)

	// Add files to the layered map
	for _, file := range filesToAdd {
		if err := s.l.Add(file); err != nil {
			return "", fmt.Errorf("unable to add file %s to layered map: %s", file, err)
		}
	}

	t := util.NewTar(f)
	defer t.Close()
	if err := writeToTar(t, filesToAdd, nil); err != nil {
		return "", err
	}
	return f.Name(), nil
}

// TakeSnapshotFS takes a snapshot of the filesystem, avoiding directories in the whitelist, and creates
// a tarball of the changed files.
// / からfull scanして差分をtarに書く
func (s *Snapshotter) TakeSnapshotFS() (string, error) {
	f, err := ioutil.TempFile(snapshotPathPrefix, "")
	if err != nil {
		return "", err
	}
	defer f.Close()
	t := util.NewTar(f)
	defer t.Close()

	// / からscanして差分を検知する
	filesToAdd, filesToWhiteOut, err := s.scanFullFilesystem()
	if err != nil {
		return "", err
	}

	// tarに書き込む. WhiteOutは.whで書き込む
	if err := writeToTar(t, filesToAdd, filesToWhiteOut); err != nil {
		return "", err
	}

	return f.Name(), nil
}

func (s *Snapshotter) scanFullFilesystem() ([]string, []string, error) {
	logrus.Info("Taking snapshot of full filesystem...")

	// Some of the operations that follow (e.g. hashing) depend on the file system being synced,
	// for example the hashing function that determines if files are equal uses the mtime of the files,
	// which can lag if sync is not called. Unfortunately there can still be lag if too much data needs
	// to be flushed or the disk does its own caching/buffering.
	_ = syscall.Sync()

	// s.directoryは"/", root
	// whiteoutsとlayersの末尾に新しいmapを追加する
	s.l.Snapshot()

	timer := timing.Start("Walking filesystem")
	// /からすべてのパスを一旦メモリに保持する
	memFs := map[string]*godirwalk.Dirent{}
	_ = godirwalk.Walk(s.directory, &godirwalk.Options{
		Callback: func(path string, ent *godirwalk.Dirent) error {
			// /proc/self/mountinfoとVOLUMEコマンドで指定されたパス以下にないか確認する
			// /kaniko, /var/run, /etc/mtab 以下もダメ
			if util.IsInWhitelist(path) {
				// IsDir, 存在しない場合はpathの末尾が/かどうか
				if util.IsDestDir(path) {
					logrus.Debugf("Skipping paths under %s, as it is a whitelisted directory", path)
					return filepath.SkipDir
				}
				return nil
			}
			memFs[path] = ent
			return nil
		},
		Unsorted: true,
	},
	)
	timing.DefaultRun.Stop(timer)

	// imageファイルの展開 -> layeredMapに追加の流れを踏むので
	// imageファイルの展開しか行われてない状態でここに来る
	// 実際のFSであるmemFsとメモリ上の状態であるイメージの展開前のlayeredMapの差分は
	// layeredMap - memFs は 今回削除されるだろう領域

	// s.lにはこれまでのlayerが全て保持されている.
	// whiteoutファイルを考慮したファイル群を取得する
	existingPaths := s.l.getFlattenedPathsForWhiteOut()

	// 実際のディレクトリ上にあるファイルを全て取り除く
	for p := range memFs {
		delete(existingPaths, p)
	}
	//   The paths left here are the ones that have been deleted in this layer.
	filesToWhiteOut := []string{}
	for path := range existingPaths {
		// Only add the whiteout if the directory for the file still exists.
		dir := filepath.Dir(path)
		if _, ok := memFs[dir]; ok {
			// これまでのwhiteoutsに登録されているか確認して登録されていなければ追加する
			// 追加したときにtrueを返す
			if s.l.MaybeAddWhiteout(path) {
				logrus.Infof("Adding whiteout for %s", path)
				filesToWhiteOut = append(filesToWhiteOut, path)
			}
		}
	}

	filesToAdd := []string{}
	for path := range memFs {
		if util.CheckWhitelist(path) {
			logrus.Debugf("Not adding %s to layer, as it's whitelisted", path)
			continue
		}
		// Only add changed files.
		// すべてのファイルのhashをとったりするのか..., なるほどmtimeのみのモードが理解できる
		fileChanged, err := s.l.CheckFileChange(path)
		if err != nil {
			return nil, nil, err
		}
		if fileChanged {
			logrus.Debugf("Adding %s to layer, because it was changed.", path)
			filesToAdd = append(filesToAdd, path)
		}
	}

	// Also add parent directories to keep the permission of them correctly.
	// なんか追加予定のファイルの上位ディレクトリすべて
	filesToAdd = filesWithParentDirs(filesToAdd)

	// Add files to the layered map
	for _, file := range filesToAdd {
		// ここで初めてメモリ上の状態が更新される
		if err := s.l.Add(file); err != nil {
			return nil, nil, fmt.Errorf("unable to add file %s to layered map: %s", file, err)
		}
	}

	return filesToAdd, filesToWhiteOut, nil
}

func writeToTar(t util.Tar, files, whiteouts []string) error {
	timer := timing.Start("Writing tar file")
	defer timing.DefaultRun.Stop(timer)
	// Now create the tar.
	for _, path := range whiteouts {
		if err := t.Whiteout(path); err != nil {
			return err
		}
	}
	for _, path := range files {
		if err := t.AddFileToTar(path); err != nil {
			return err
		}
	}
	return nil
}

func filesWithParentDirs(files []string) []string {
	filesSet := map[string]bool{}

	for _, file := range files {
		file = filepath.Clean(file)
		filesSet[file] = true

		for _, dir := range util.ParentDirectories(file) {
			dir = filepath.Clean(dir)
			filesSet[dir] = true
		}
	}

	newFiles := []string{}
	for file := range filesSet {
		newFiles = append(newFiles, file)
	}

	return newFiles
}
