// files.go：日志文件相关工具（检查存在/权限、创建目录、按时间戳取名等）。
package logger

import (
	"fmt"
	"os"
)

// checkNotExist —— 用途：判断路径是否不存在。
func checkNotExist(src string) bool {
	_, err := os.Stat(src)
	return os.IsNotExist(err)
}

func checkPermission(src string) bool {
	_, err := os.Stat(src)
	return os.IsPermission(err)
}

func isNotExistMkDir(src string) error {
	if checkNotExist(src) {
		return mkDir(src)
	}
	return nil

}

func mkDir(src string) error {
	return os.MkdirAll(src, os.ModePerm)
}

func mustOpen(fileName, dir string) (*os.File, error) {
	if checkPermission(dir) {
		return nil, fmt.Errorf("permission denied dir: %s", dir)
	}

	if err := isNotExistMkDir(dir); err != nil {
		return nil, fmt.Errorf("error during make dir %s, err: %s", dir, err)
	}

	f, err := os.OpenFile(dir+string(os.PathSeparator)+fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("fail to open file, err: %s", err)
	}

	return f, nil
}
