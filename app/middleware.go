package app

import (
	"context"
	"github.com/go-ap/activitypub/storage"
	"github.com/go-ap/fedbox/internal/log"
	"github.com/sirupsen/logrus"
	"net/http"
)

type CtxtKey string

var RepositoryCtxtKey = CtxtKey("__repo")

func Repo (loader storage.Loader) func (next http.Handler) http.Handler{
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			newCtx := context.WithValue(ctx, RepositoryCtxtKey, loader)
			next.ServeHTTP(w, r.WithContext(newCtx))
		}
		return http.HandlerFunc(fn)
	}
}