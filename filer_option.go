package goseaweedfs

type FilerOption func(f *Filer)

func WithFilerAuthKey(authKey string) FilerOption {
	return func(f *Filer) {
		f.authKey = authKey
	}
}
