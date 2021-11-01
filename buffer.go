package goseaweedfs

type FileMark rune

const (
	EmptyMark FileMark = '\U0000FFFF'
)

func (m FileMark) String() string {
	return string(m)
}

func (m FileMark) Bytes() []byte {
	return []byte(m.String())
}

func IsFileMarkBytes(data []byte, m FileMark) bool {
	return string(data) == m.String()
}
