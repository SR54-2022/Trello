package clients

import (
	"errors"
	"net/url"
	"time"
	"user-service/domain"

	"github.com/sony/gobreaker"
)

func HandleHttpReqErr(err error, reqUrl string, method string, timeout time.Duration) error {
	// request failed because breaker wasn't in the closed state
	// and we didn't even try to send it
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return err
	}
	// the request was sent
	urlErr, ok := err.(*url.Error)
	if !ok {
		return domain.ErrUnknown{
			InnerErr: err,
		}
	}
	if urlErr.Timeout() {
		return domain.ErrClientSideTimeout{
			URL:        reqUrl,
			Method:     method,
			MaxTimeout: timeout,
		}
	}
	return domain.ErrConnecting{
		Err: urlErr,
	}
}
