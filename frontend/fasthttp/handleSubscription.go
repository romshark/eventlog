package fasthttp

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/fasthttp/websocket"
	"github.com/valyala/fasthttp"
)

func (s *Server) handleSubscription(ctx *fasthttp.RequestCtx) error {
	return s.wsUpgrader.Upgrade(ctx, func(conn *websocket.Conn) {
		ch, closeSub := s.eventLog.Subscribe()

		pingTicker := time.NewTicker(s.wsPingInterval)
		closed := uint32(0)
		closeConn := func() {
			if !atomic.CompareAndSwapUint32(&closed, 0, 1) {
				// Already closed
				return
			}
			closeSub()
			pingTicker.Stop()
			conn.Close()

			s.lock.Lock()
			delete(s.wsConns, conn)
			s.lock.Unlock()
		}
		write := func(msgType int, msg []byte) error {
			if err := conn.SetWriteDeadline(
				time.Now().Add(s.wsWriteTimeout),
			); err != nil {
				return err
			}
			return conn.WriteMessage(msgType, msg)
		}

		defer closeConn()

		s.lock.Lock()
		s.wsConns[conn] = closeConn
		s.lock.Unlock()

		go func() {
			for {
				select {
				case version, ok := <-ch:
					// Send version update notification
					if !ok {
						return
					}

					// Encode version to hex byte string
					msg := []byte(strconv.FormatUint(version, 16))

					if err := write(
						websocket.BinaryMessage,
						msg,
					); err != nil {
						return
					}
				case _, ok := <-pingTicker.C:
					// Send ping control message
					if !ok {
						return
					}

					if err := write(
						websocket.PingMessage,
						nil,
					); err != nil {
						return
					}
				}
			}
		}()

		// Setup listener
		conn.SetReadLimit(1)
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			s.logErr.Printf(
				"ERR: disabling websocket read timeout: %s\n",
				err,
			)
		}
		_, _, _ = conn.NextReader()
		closeConn()
	})
}
