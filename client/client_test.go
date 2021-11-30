package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	apifasthttp "github.com/romshark/eventlog/api/fasthttp"
	"github.com/romshark/eventlog/client"
	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/hex"

	"github.com/fasthttp/websocket"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

//go:generate go run -mod=mod github.com/golang/mock/mockgen -source ../eventlog/eventlog.go -package client_test -destination ./client_gen_test.go EventLogger

const FastHTTPMaxReadBatchSize = 2

func test(t *testing.T, f func(*testing.T, Setup)) {
	setupHTTP := func(t *testing.T) (s Setup) {
		s.Name = "HTTP"
		ctrl := gomock.NewController(t)
		s.Logger = NewMockEventLogger(ctrl)

		inMemListener := fasthttputil.NewInmemoryListener()
		t.Cleanup(func() {
			require.NoError(t, inMemListener.Close())
		})

		server := &fasthttp.Server{
			Handler: apifasthttp.New(
				log.New(os.Stderr, "ERR", log.LstdFlags),
				eventlog.New(s.Logger),
				FastHTTPMaxReadBatchSize,
			).Serve,
		}

		go func() {
			require.NoError(t, server.Serve(inMemListener))
		}()

		s.Client = client.New(
			client.NewHTTP(
				"localhost",
				log.New(os.Stderr, "ERR", log.LstdFlags),
				&fasthttp.Client{
					Dial: func(addr string) (net.Conn, error) {
						return inMemListener.Dial()
					},
				},
				&websocket.Dialer{
					NetDialContext: func(
						ctx context.Context,
						network string,
						addr string,
					) (net.Conn, error) {
						return inMemListener.Dial()
					},
				},
			),
		)
		return
	}

	setupInmem := func(t *testing.T) (s Setup) {
		s.Name = "Inmem"
		ctrl := gomock.NewController(t)
		s.Logger = NewMockEventLogger(ctrl)
		s.Client = client.New(client.NewInmem(eventlog.New(s.Logger)))
		return
	}

	t.Run("HTTP", func(t *testing.T) { f(t, setupHTTP(t)) })
	t.Run("Inmem", func(t *testing.T) { f(t, setupInmem(t)) })
}

func TestMetadata(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		c1 := s.Logger.EXPECT().MetadataLen().Times(1).Return(2)
		s.Logger.EXPECT().ScanMetadata(Callback(func(cb interface{}) {
			f := cb.(func(field, value string) bool)
			require.True(t, f("foo", "bar"))
			require.True(t, f("baz", "fuz"))
		})).Times(1).After(c1)

		fetched, err := s.Client.Metadata(context.Background())
		r.NoError(err)
		r.Len(fetched, 2)

		r.Contains(fetched, "foo")
		r.Equal("bar", fetched["foo"])

		r.Contains(fetched, "baz")
		r.Equal("fuz", fetched["baz"])

	})
}

func TestAppend(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		expTime := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)
		expPrevVers := uint64(123)
		expVers := uint64(456)

		s.Logger.EXPECT().Append(eventlog.EventData{
			Label:       []byte("foo"),
			PayloadJSON: []byte(`{"bar":"baz"}`),
		}).Times(1).Return(
			expPrevVers,
			expVers,
			expTime,
			error(nil),
		)

		pv, v, tm, err := s.Client.Append(
			context.Background(),
			makeEvent(t, "foo", Doc{"bar": "baz"}),
		)
		r.NoError(err)
		r.Equal(expPrevVers, fromHex(t, pv))
		r.Equal(expVers, fromHex(t, v))
		r.Equal(expTime, tm)
	})
}

func TestAppendMulti(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		expTime := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)
		expPrevVers := uint64(123)
		expFirstVers := uint64(456)
		expVers := uint64(789)

		s.Logger.EXPECT().AppendMulti(eventlog.EventData{
			Label:       []byte("first"),
			PayloadJSON: []byte(`{"foo":"bar"}`),
		}, eventlog.EventData{
			Label:       []byte("second"),
			PayloadJSON: []byte(`{"bar":"baz"}`),
		}).Times(1).Return(
			expPrevVers,
			expFirstVers,
			expVers,
			expTime,
			error(nil),
		)

		pv, fv, v, tm, err := s.Client.AppendMulti(
			context.Background(),
			makeEvent(t, "first", Doc{"foo": "bar"}),
			makeEvent(t, "second", Doc{"bar": "baz"}),
		)
		r.NoError(err)
		r.Equal(expPrevVers, fromHex(t, pv))
		r.Equal(expFirstVers, fromHex(t, fv))
		r.Equal(expVers, fromHex(t, v))
		r.Equal(expTime, tm)
	})
}

func TestAppendCheck(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		assumed := fmt.Sprintf("%x", uint64(123))
		expTime := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)
		expVers := uint64(456)

		s.Logger.EXPECT().AppendCheck(
			fromHex(t, assumed),
			eventlog.EventData{
				Label:       []byte("foo"),
				PayloadJSON: []byte(`{"bar":"baz"}`),
			},
		).Times(1).Return(
			expVers,
			expTime,
			error(nil),
		)

		v, tm, err := s.Client.AppendCheck(
			context.Background(),
			assumed,
			makeEvent(t, "foo", Doc{"bar": "baz"}),
		)
		r.NoError(err)
		r.Equal(expVers, fromHex(t, v))
		r.Equal(expTime, tm)
	})
}

func TestAppendCheckMulti(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		assumed := fmt.Sprintf("%x", uint64(123))
		expTime := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)
		expFirstVers := uint64(456)
		expVers := uint64(789)

		s.Logger.EXPECT().AppendCheckMulti(
			fromHex(t, assumed),
			eventlog.EventData{
				Label:       []byte("first"),
				PayloadJSON: []byte(`{"foo":"bar"}`),
			}, eventlog.EventData{
				Label:       []byte("second"),
				PayloadJSON: []byte(`{"bar":"baz"}`),
			},
		).Times(1).Return(
			expFirstVers,
			expVers,
			expTime,
			error(nil),
		)

		fv, v, tm, err := s.Client.AppendCheckMulti(
			context.Background(),
			assumed,
			makeEvent(t, "first", Doc{"foo": "bar"}),
			makeEvent(t, "second", Doc{"bar": "baz"}),
		)
		r.NoError(err)
		r.Equal(expFirstVers, fromHex(t, fv))
		r.Equal(expVers, fromHex(t, v))
		r.Equal(expTime, tm)
	})
}

func TestErrAppendMultiNoEvents(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		pv, fv, v, tm, err := s.Client.AppendMulti(context.Background())
		r.Error(err)
		RequireErr(t, err, client.ErrNoEvents)
		r.Zero(pv)
		r.Zero(fv)
		r.Zero(v)
		r.Zero(tm)

		fv, v, tm, err = s.Client.AppendCheckMulti(context.Background(), "1")
		r.Error(err)
		RequireErr(t, err, client.ErrNoEvents)
		r.Zero(pv)
		r.Zero(fv)
		r.Zero(v)
		r.Zero(tm)
	})
}

func TestErrAppendCheckNoAssumedVersion(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		v, tm, err := s.Client.AppendCheck(
			context.Background(),
			"",
			makeEvent(t, "foo", Doc{"foo": "bar"}),
		)
		RequireErr(t, err, client.ErrInvalidVersion)
		r.Zero(v)
		r.Zero(tm)

		fv, v, tm, err := s.Client.AppendCheckMulti(
			context.Background(),
			"",
			makeEvent(t, "foo", Doc{"foo": "bar"}),
		)
		RequireErr(t, err, client.ErrInvalidVersion)
		r.Zero(v)
		r.Zero(fv)
		r.Zero(tm)
	})
}

func TestErrAppendPayloadExceedsLimit(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		e := []eventlog.EventData{{
			Label:       []byte(""),
			PayloadJSON: []byte(`{"k":"too long"}`),
		}, {
			Label:       []byte(""),
			PayloadJSON: []byte(`{"k":"second"}`),
		}}

		{
			s.Logger.EXPECT().
				AppendCheck(uint64(0), e[0]).
				Times(1).
				Return(
					uint64(0),
					time.Time{},
					eventlog.ErrPayloadSizeLimitExceeded,
				)

			v, tm, err := s.Client.AppendCheck(context.Background(), "0", e[0])
			RequireErr(t, client.ErrPayloadSizeLimitExceeded, err)
			r.Zero(v)
			r.Zero(tm)
		}

		{
			s.Logger.EXPECT().
				AppendCheckMulti(uint64(0), e).
				Times(1).
				Return(
					uint64(0),
					uint64(0),
					time.Time{},
					eventlog.ErrPayloadSizeLimitExceeded,
				)

			vf, v, tm, err := s.Client.AppendCheckMulti(
				context.Background(), "0", e...,
			)
			RequireErr(t, client.ErrPayloadSizeLimitExceeded, err)
			r.Zero(vf)
			r.Zero(v)
			r.Zero(tm)
		}

		{
			s.Logger.EXPECT().
				Append(e[0]).
				Times(1).
				Return(
					uint64(0),
					uint64(0),
					time.Time{},
					eventlog.ErrPayloadSizeLimitExceeded,
				)

			vp, v, tm, err := s.Client.Append(context.Background(), e[0])
			RequireErr(t, client.ErrPayloadSizeLimitExceeded, err)
			r.Zero(vp)
			r.Zero(v)
			r.Zero(tm)
		}

		{
			s.Logger.EXPECT().
				AppendMulti(e).
				Times(1).
				Return(
					uint64(0),
					uint64(0),
					uint64(0),
					time.Time{},
					eventlog.ErrPayloadSizeLimitExceeded,
				)

			vp, vf, v, tm, err := s.Client.AppendMulti(
				context.Background(), e...,
			)
			RequireErr(t, client.ErrPayloadSizeLimitExceeded, err)
			r.Zero(vp)
			r.Zero(vf)
			r.Zero(v)
			r.Zero(tm)
		}
	})
}

func TestScan(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		expEvents := []client.Event{{
			Time:            time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
			VersionPrevious: "0",
			Version:         "1",
			VersionNext:     "2",
			EventData: eventlog.EventData{
				Label:       []byte("first"),
				PayloadJSON: []byte(`{"i":1}`),
			},
		}, {
			Time:            time.Date(2021, 1, 1, 1, 2, 1, 0, time.UTC),
			VersionPrevious: "1",
			Version:         "2",
			VersionNext:     "3",
			EventData: eventlog.EventData{
				Label:       []byte("second"),
				PayloadJSON: []byte(`{"i":2}`),
			},
		}, {
			Time:            time.Date(2021, 1, 1, 1, 3, 1, 0, time.UTC),
			VersionPrevious: "2",
			Version:         "3",
			VersionNext:     "4",
			EventData: eventlog.EventData{
				Label:       []byte("third"),
				PayloadJSON: []byte(`{"i":3}`),
			},
		}, {
			Time:            time.Date(2021, 1, 1, 1, 4, 1, 0, time.UTC),
			VersionPrevious: "3",
			Version:         "4",
			VersionNext:     "5",
			EventData: eventlog.EventData{
				Label:       []byte("fourth"),
				PayloadJSON: []byte(`{"i":4}`),
			},
		}, {
			Time:            time.Date(2021, 1, 1, 1, 5, 1, 0, time.UTC),
			VersionPrevious: "4",
			Version:         "5",
			VersionNext:     "0",
			EventData: eventlog.EventData{
				Label:       []byte("fifth"),
				PayloadJSON: []byte(`{"i":5}`),
			},
		}}

		switch s.Name {
		case "HTTP":
			// For the HTTP setup 3 separate calls are expected
			// since the length of a batch is limited to
			// FastHTTPMaxReadBatchSize
			c1 := s.Logger.EXPECT().Scan(
				uint64(1),
				false,
				Callback(func(f interface{}) {
					callScanCallback(t, f, expEvents[0], ExpectNoErr)
					callScanCallback(t, f, expEvents[1], ExpectErr)
				}),
			).Times(1).Return(nil)

			c2 := s.Logger.EXPECT().Scan(
				uint64(3),
				false,
				Callback(func(f interface{}) {
					callScanCallback(t, f, expEvents[2], ExpectNoErr)
					callScanCallback(t, f, expEvents[3], ExpectErr)
				}),
			).Times(1).Return(nil).After(c1)

			s.Logger.EXPECT().Scan(
				uint64(5),
				false,
				Callback(func(f interface{}) {
					callScanCallback(t, f, expEvents[4], ExpectErr)
				}),
			).Times(1).Return(nil).After(c2)

		case "Inmem":
			s.Logger.EXPECT().Scan(
				uint64(1),
				false,
				Callback(func(f interface{}) {
					callScanCallback(t, f, expEvents[0], ExpectNoErr)
					callScanCallback(t, f, expEvents[1], ExpectNoErr)
					callScanCallback(t, f, expEvents[2], ExpectNoErr)
					callScanCallback(t, f, expEvents[3], ExpectNoErr)
					callScanCallback(t, f, expEvents[4], ExpectNoErr)
				}),
			).Times(1).Return(nil)

		default:
			t.Fatalf("unsupported setup: %q", s.Name)
		}

		i := 0
		require.NoError(t, s.Client.Scan(
			context.Background(),
			"1",
			false,
			false,
			func(e client.Event) error {
				require.Equal(t, expEvents[i], e, "mismatch at %d", i)
				i++
				return nil
			},
		))
		require.Equal(t, len(expEvents), i)
	})
}

func TestScanReverse(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		expEvents := []client.Event{{
			Time:            time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
			VersionPrevious: "0",
			Version:         "1",
			VersionNext:     "2",
			EventData: eventlog.EventData{
				Label:       []byte("first"),
				PayloadJSON: []byte(`{"i":1}`),
			},
		}, {
			Time:            time.Date(2021, 1, 1, 1, 2, 1, 0, time.UTC),
			VersionPrevious: "1",
			Version:         "2",
			VersionNext:     "3",
			EventData: eventlog.EventData{
				Label:       []byte("second"),
				PayloadJSON: []byte(`{"i":2}`),
			},
		}, {
			Time:            time.Date(2021, 1, 1, 1, 3, 1, 0, time.UTC),
			VersionPrevious: "2",
			Version:         "3",
			VersionNext:     "0",
			EventData: eventlog.EventData{
				Label:       []byte("third"),
				PayloadJSON: []byte(`{"i":3}`),
			},
		}}

		switch s.Name {
		case "HTTP":
			// For the HTTP setup 2 separate calls are expected
			// since the length of a batch is limited to
			// FastHTTPMaxReadBatchSize

			c1 := s.Logger.EXPECT().Scan(
				uint64(3),
				true,
				Callback(func(f interface{}) {
					callScanCallback(t, f, expEvents[2], ExpectNoErr)
					callScanCallback(t, f, expEvents[1], ExpectErr)
				}),
			).Times(1).Return(nil)

			s.Logger.EXPECT().Scan(
				uint64(1),
				true,
				Callback(func(f interface{}) {
					callScanCallback(t, f, expEvents[0], ExpectErr)
				}),
			).Times(1).Return(nil).After(c1)

		case "Inmem":
			s.Logger.EXPECT().Scan(
				uint64(3),
				true,
				Callback(func(f interface{}) {
					callScanCallback(t, f, expEvents[2], ExpectNoErr)
					callScanCallback(t, f, expEvents[1], ExpectNoErr)
					callScanCallback(t, f, expEvents[0], ExpectNoErr)
				}),
			).Times(1).Return(nil)

		default:
			t.Fatalf("unsupported setup: %q", s.Name)
		}

		i := len(expEvents) - 1
		counter := 0
		require.NoError(t, s.Client.Scan(
			context.Background(),
			"3",
			true, // Reverse
			false,
			func(e client.Event) error {
				require.Equal(t, expEvents[i], e, "mismatch at %d", i)
				i--
				counter++
				return nil
			},
		))
		require.Equal(t, len(expEvents), counter)
	})
}

func TestScanSkipFirst(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		expEvents := []client.Event{{
			Time:            time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
			VersionPrevious: "0",
			Version:         "1",
			VersionNext:     "2",
			EventData: eventlog.EventData{
				Label:       []byte("first"),
				PayloadJSON: []byte(`{"i":1}`),
			},
		}, {
			Time:            time.Date(2021, 1, 1, 1, 2, 1, 0, time.UTC),
			VersionPrevious: "1",
			Version:         "2",
			VersionNext:     "0",
			EventData: eventlog.EventData{
				Label:       []byte("second"),
				PayloadJSON: []byte(`{"i":2}`),
			},
		}}

		s.Logger.EXPECT().Scan(
			uint64(1),
			false,
			Callback(func(f interface{}) {
				callScanCallback(t, f, expEvents[0], ExpectNoErr)
				switch s.Name {
				case "HTTP":
					// The HTTP client will force-stop using an error
					callScanCallback(t, f, expEvents[1], ExpectErr)
				case "Inmem":
					callScanCallback(t, f, expEvents[1], ExpectNoErr)
				}
			})).
			Times(1).
			Return(nil)

		counter := 0
		require.NoError(t, s.Client.Scan(
			context.Background(),
			"1",
			false, // Reverse
			true,  // Skip first
			func(e client.Event) error {
				require.Equal(t, expEvents[1], e)
				counter++
				return nil
			},
		))
		require.Equal(t, 1, counter)
	})
}

func TestScanReverseSkipFirst(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		expEvents := []client.Event{{
			Time:            time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
			VersionPrevious: "0",
			Version:         "1",
			VersionNext:     "2",
			EventData: eventlog.EventData{
				Label:       []byte("first"),
				PayloadJSON: []byte(`{"i":1}`),
			},
		}, {
			Time:            time.Date(2021, 1, 1, 1, 2, 1, 0, time.UTC),
			VersionPrevious: "1",
			Version:         "2",
			VersionNext:     "0",
			EventData: eventlog.EventData{
				Label:       []byte("second"),
				PayloadJSON: []byte(`{"i":2}`),
			},
		}}

		s.Logger.EXPECT().Scan(
			uint64(2),
			true,
			Callback(func(f interface{}) {
				callScanCallback(t, f, expEvents[1], ExpectNoErr)
				switch s.Name {
				case "HTTP":
					// The HTTP client will force-stop using an error
					callScanCallback(t, f, expEvents[0], ExpectErr)
				case "Inmem":
					callScanCallback(t, f, expEvents[0], ExpectNoErr)
				}
			})).
			Times(1).
			Return(nil)

		counter := 0
		require.NoError(t, s.Client.Scan(
			context.Background(),
			"2",
			true, // Reverse
			true, // Skip first
			func(e client.Event) error {
				require.Equal(t, expEvents[0], e)
				counter++
				return nil
			},
		))
		require.Equal(t, 1, counter)
	})
}

func TestVersion(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		s.Logger.EXPECT().Version().Times(1).Return(uint64(42))

		v, err := s.Client.Version(context.Background())
		require.NoError(t, err)
		require.Equal(t, "2a", v)
	})
}

func TestVersionInitial(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		s.Logger.EXPECT().VersionInitial().Times(1).Return(uint64(42))

		v, err := s.Client.VersionInitial(context.Background())
		require.NoError(t, err)
		require.Equal(t, "2a", v)
	})
}

func TestListen(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		versionChan1 := make(chan client.Version, 1)
		go func() {
			if err := s.Client.Listen(context.Background(), func(v client.Version) {
				versionChan1 <- client.Version(v)
			}); err != nil {
				panic(err)
			}
		}()

		versionChan2 := make(chan client.Version, 1)
		go func() {
			if err := s.Client.Listen(context.Background(), func(v client.Version) {
				versionChan2 <- client.Version(v)
			}); err != nil {
				panic(err)
			}
		}()

		s.Logger.EXPECT().Append(eventlog.EventData{
			Label:       []byte(""),
			PayloadJSON: []byte(`{"foo":"bar"}`),
		}).Times(1).Return(
			uint64(0),
			uint64(1),
			time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
			error(nil),
		)

		time.Sleep(100 * time.Millisecond)
		_, v, _, err := s.Client.Append(
			context.Background(),
			makeEvent(t, "", Doc{"foo": "bar"}),
		)
		r.NoError(err)

		r.Equal(v, <-versionChan1)
		r.Equal(v, <-versionChan2)
	})
}

func TestListenCancel(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		chanErr := make(chan error, 1)
		versionTriggered := uint32(0)

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			chanErr <- s.Client.Listen(ctx, func(v client.Version) {
				atomic.AddUint32(&versionTriggered, 1)
			})
		}()

		cancel()
		r.Equal(context.Canceled, <-chanErr)
		r.Zero(atomic.LoadUint32(&versionTriggered))
	})
}

func TestTryAppend(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		c1 := s.Logger.EXPECT().AppendCheck(
			uint64(1),
			eventlog.EventData{
				Label:       []byte("x"),
				PayloadJSON: []byte(`{"x":"y"}`),
			},
		).Times(1).Return(
			uint64(0),
			time.Time{},
			eventlog.ErrMismatchingVersions,
		)

		s.Logger.EXPECT().AppendCheck(
			uint64(2),
			eventlog.EventData{
				Label:       []byte("x"),
				PayloadJSON: []byte(`{"x":"y"}`),
			},
		).Times(1).Return(
			uint64(3),
			time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
			error(nil),
		).After(c1)

		syncCalled := 0
		transactionCalled := 0

		pv, v, tm, err := s.Client.TryAppend(
			context.Background(),
			"1",
			func() (eventlog.EventData, error) {
				// Transaction
				transactionCalled++
				return makeEvent(t, "x", Doc{"x": "y"}), nil
			},
			func() (client.Version, error) {
				// Sync
				syncCalled++
				return "2", nil
			},
		)
		r.NoError(err)
		r.Equal("2", pv)
		r.Equal("3", v)
		r.Equal(time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), tm)

		r.Equal(1, syncCalled)
		r.Equal(2, transactionCalled)
	})
}

func TestTryAppendMulti(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		c1 := s.Logger.EXPECT().AppendCheckMulti(
			uint64(1),
			[]eventlog.EventData{{
				Label:       []byte("a"),
				PayloadJSON: []byte(`{"a":"b"}`),
			}, {
				Label:       []byte("b"),
				PayloadJSON: []byte(`{"b":"c"}`),
			}},
		).Times(1).Return(
			uint64(0), // First version
			uint64(0), // Version
			time.Time{},
			eventlog.ErrMismatchingVersions,
		)

		s.Logger.EXPECT().AppendCheckMulti(
			uint64(2),
			[]eventlog.EventData{{
				Label:       []byte("a"),
				PayloadJSON: []byte(`{"a":"b"}`),
			}, {
				Label:       []byte("b"),
				PayloadJSON: []byte(`{"b":"c"}`),
			}},
		).Times(1).Return(
			uint64(3), // First version
			uint64(4), // Version
			time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
			error(nil),
		).After(c1)

		syncCalled := 0
		transactionCalled := 0

		pv, fv, v, tm, err := s.Client.TryAppendMulti(
			context.Background(),
			"1",
			func() ([]eventlog.EventData, error) {
				// Transaction
				transactionCalled++
				return []eventlog.EventData{
					makeEvent(t, "a", Doc{"a": "b"}),
					makeEvent(t, "b", Doc{"b": "c"}),
				}, nil
			},
			func() (client.Version, error) {
				// Sync
				syncCalled++
				return "2", nil
			},
		)
		r.NoError(err)
		r.Equal("2", pv)
		r.Equal("3", fv)
		r.Equal("4", v)
		r.Equal(time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), tm)

		r.Equal(1, syncCalled)
		r.Equal(2, transactionCalled)
	})
}

func TestTryAppendTransactionErr(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		errTransaction := errors.New("transaction error")
		transactionCalled := 0

		pv, v, tm, err := s.Client.TryAppend(
			context.Background(),
			"1",
			func() (eventlog.EventData, error) {
				// Transaction
				transactionCalled++
				return eventlog.EventData{}, errTransaction
			},
			func() (client.Version, error) {
				// Sync
				t.Fatal("this callback must not be invoked")
				return "", nil
			},
		)

		r.Error(err)
		r.Equal(errTransaction, err)
		r.Equal(1, transactionCalled)
		r.Zero(pv)
		r.Zero(v)
		r.Zero(tm)

		transactionCalled = 0

		pv, fv, v, tm, err := s.Client.TryAppendMulti(
			context.Background(),
			"1",
			func() ([]eventlog.EventData, error) {
				// Transaction
				transactionCalled++
				return []eventlog.EventData{}, errTransaction
			},
			func() (client.Version, error) {
				// Sync
				t.Fatal("this callback must not be invoked")
				return "", nil
			},
		)

		r.Error(err)
		r.Equal(errTransaction, err)
		r.Equal(1, transactionCalled)
		r.Zero(pv)
		r.Zero(fv)
		r.Zero(v)
		r.Zero(tm)
	})
}

func TestTryAppendSyncErr(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		c1 := s.Logger.EXPECT().AppendCheck(
			uint64(1),
			eventlog.EventData{
				Label:       []byte("x"),
				PayloadJSON: []byte(`{"x":"y"}`),
			},
		).Times(1).Return(
			uint64(0),
			time.Time{},
			eventlog.ErrMismatchingVersions,
		)

		s.Logger.EXPECT().AppendCheckMulti(
			uint64(1),
			[]eventlog.EventData{{
				Label:       []byte("a"),
				PayloadJSON: []byte(`{"a":"b"}`),
			}, {
				Label:       []byte("b"),
				PayloadJSON: []byte(`{"b":"c"}`),
			}},
		).Times(1).Return(
			uint64(0), // First version
			uint64(0), // Version
			time.Time{},
			eventlog.ErrMismatchingVersions,
		).After(c1)

		errSync := errors.New("sync error")

		syncCalled := 0
		transactionCalled := 0

		pv, v, tm, err := s.Client.TryAppend(
			context.Background(),
			"1",
			func() (eventlog.EventData, error) {
				// Transaction
				transactionCalled++
				return makeEvent(t, "x", Doc{"x": "y"}), nil
			},
			func() (client.Version, error) {
				// Sync
				syncCalled++
				return "", errSync
			},
		)
		r.Error(err)
		r.Equal(errSync, err)
		r.Zero(pv)
		r.Zero(v)
		r.Zero(tm)
		r.Equal(1, syncCalled)
		r.Equal(1, transactionCalled)

		syncCalled = 0
		transactionCalled = 0

		pv, fv, v, tm, err := s.Client.TryAppendMulti(
			context.Background(),
			"1",
			func() ([]eventlog.EventData, error) {
				// Transaction
				transactionCalled++
				return []eventlog.EventData{
					makeEvent(t, "a", Doc{"a": "b"}),
					makeEvent(t, "b", Doc{"b": "c"}),
				}, nil
			},
			func() (client.Version, error) {
				// Sync
				syncCalled++
				return "", errSync
			},
		)
		r.Error(err)
		r.Equal(errSync, err)
		r.Zero(pv)
		r.Zero(fv)
		r.Zero(v)
		r.Zero(tm)
		r.Equal(1, syncCalled)
		r.Equal(1, transactionCalled)
	})
}

type Event struct {
	client.Event
	Payload Doc
}

func fromHex(t *testing.T, s string) uint64 {
	i, err := hex.ReadUint64([]byte(s))
	require.NoError(t, err)
	return i
}

func makeEvent(t *testing.T, label string, d Doc) client.EventData {
	b, err := json.Marshal(d)
	require.NoError(t, err)
	return eventlog.EventData{
		Label:       []byte(label),
		PayloadJSON: b,
	}
}

type Doc map[string]interface{}

type Setup struct {
	Name   string
	Logger *MockEventLogger
	Client *client.Client
}

type Callback func(fn interface{})

func (m Callback) Matches(fn interface{}) bool {
	m(fn)
	return true
}
func (Callback) String() string { return "is a callback lambda function" }

func RequireErr(t *testing.T, expected, actual error) {
	t.Helper()
	require.Error(t, actual)
	require.True(
		t,
		errors.Is(actual, expected),
		"unexpected error: (%T) %s", actual, actual.Error(),
	)
}

func Translate(t *testing.T, from client.Event) (to eventlog.Event) {
	to.PayloadJSON = make([]byte, len(from.PayloadJSON))
	copy(to.PayloadJSON, from.PayloadJSON)

	to.Label = make([]byte, len(from.Label))
	copy(to.Label, from.Label)

	to.Timestamp = uint64(from.Time.Unix())
	to.Version = fromHex(t, from.Version)
	to.VersionPrevious = fromHex(t, from.VersionPrevious)
	to.VersionNext = fromHex(t, from.VersionNext)

	return
}

// callScanCallback expects cb to be a eventlog.ScanFn function
// calls it passing passEvent. If expectStopErr == true the scan-stop error
// is expected to be returned, otherwise nil is expected.
func callScanCallback(
	t *testing.T,
	cb interface{},
	passEvent client.Event,
	errExpectation expectedError,
) {
	t.Helper()
	var cbType eventlog.ScanFn
	require.IsType(t, cbType, cb)
	f := cb.(eventlog.ScanFn)
	if errExpectation == ExpectErr {
		err := f(Translate(t, passEvent))
		require.Error(t, err)
		require.Equal(t, "scan stopped", err.Error())
	} else {
		require.NoError(t, f(Translate(t, passEvent)))
	}
}

type expectedError bool

const (
	ExpectErr   expectedError = true
	ExpectNoErr expectedError = false
)
