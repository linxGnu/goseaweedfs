package goseaweedfs

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

func makeURL(scheme, host, path string, args url.Values) string {
	u := url.URL{
		Scheme: scheme,
		Host:   host,
		Path:   path,
	}
	if args != nil {
		u.RawQuery = args.Encode()
	}
	return u.String()
}

func valid(c rune) bool {
	return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || '.' == c || '-' == c || '_' == c
}

func normalizeName(st string) string {
	for _, _c := range st {
		if !valid(_c) {
			var sb strings.Builder
			sb.Grow(len(st))

			for _, c := range st {
				if valid(c) {
					_, _ = sb.WriteRune(c)
				}
			}

			return sb.String()
		}
	}
	return st
}

func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(ioutil.Discard, body)
	_ = body.Close()
}

func normalize(values url.Values) url.Values {
	if values == nil {
		values = make(url.Values)
	}
	return values
}

func readAll(r *http.Response) (body []byte, statusCode int, err error) {
	statusCode = r.StatusCode
	body, err = ioutil.ReadAll(r.Body)
	r.Body.Close()
	return
}
