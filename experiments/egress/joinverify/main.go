// joinverify replays a Turnstile token through gartic's real join handshake and
// reports whether the token is actually accepted.
//
// It is a trimmed copy of the pipeline's cmd/joindebug, reduced to the one
// combination gartic currently accepts (public join, platform 0, idioma 19) and
// packaged so it can run on a CI runner, where the bot repo is not available.
//
// Why Go and not Python: a Python websocket-client handshake to
// serverNN.gartic.io is answered with Cloudflare 403 purely on TLS fingerprint,
// from the very same IP where this client gets a clean 101. Driving the socket
// from inside the Camoufox page instead crashes Camoufox v135's Juggler
// connection. Go's TLS passes and costs nothing extra — it is preinstalled on
// every GitHub-hosted runner.
//
// Usage: joinverify <token>
// Prints exactly one line: JOINED | REJECTED code=N | ERROR:<reason> | TIMEOUT
package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
	"(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

func run(token string) string {
	transport := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{Timeout: 15 * time.Second, Transport: transport}

	request, _ := http.NewRequest("GET", "https://gartic.io/server/?check=1&v3=1&lang=19", nil)
	request.Header.Set("User-Agent", userAgent)
	request.Header.Set("Origin", "https://gartic.io")
	request.Header.Set("Referer", "https://gartic.io/")
	response, err := client.Do(request)
	if err != nil {
		return "ERROR:discover:" + err.Error()
	}
	raw, _ := io.ReadAll(response.Body)
	response.Body.Close()
	body := string(raw)
	if !strings.Contains(body, "?c=") || !strings.Contains(body, "https://") {
		// The status and a body snippet distinguish "gartic refused this IP"
		// from "the endpoint changed shape", which look identical otherwise.
		snippet := strings.Map(func(r rune) rune {
			if r == '\n' || r == '\r' || r == '\t' {
				return ' '
			}
			return r
		}, body)
		if len(snippet) > 120 {
			snippet = snippet[:120]
		}
		return fmt.Sprintf("ERROR:no-server:status=%d:body=%q", response.StatusCode, snippet)
	}
	server := strings.Split(strings.Split(body, "https://")[1], ".")[0]
	code := strings.TrimSpace(strings.Split(body, "?c=")[1])

	var cookieParts []string
	for _, raw := range response.Header.Values("Set-Cookie") {
		pair := strings.Split(raw, ";")[0]
		if pair != "" && !strings.HasSuffix(pair, "=") {
			cookieParts = append(cookieParts, pair)
		}
	}

	url := fmt.Sprintf("wss://%s.gartic.io/socket.io/?c=%s&EIO=3&transport=websocket", server, code)
	header := http.Header{}
	header.Set("User-Agent", userAgent)
	header.Set("Origin", "https://gartic.io")
	header.Set("Referer", "https://gartic.io/")
	if len(cookieParts) > 0 {
		header.Set("Cookie", strings.Join(cookieParts, "; "))
	}

	dialer := websocket.Dialer{
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: true},
		HandshakeTimeout: 10 * time.Second,
	}
	socket, dialResponse, err := dialer.Dial(url, header)
	if err != nil {
		status := 0
		if dialResponse != nil {
			status = dialResponse.StatusCode
		}
		return fmt.Sprintf("ERROR:ws-dial:status=%d:%v", status, err)
	}
	defer socket.Close()

	join := fmt.Sprintf(`42[1,{"v":20000,"token":"%s","nick":"probe","avatar":0,"platform":0,"idioma":19}]`, token)
	// One deadline for the whole exchange. Re-arming a short deadline and
	// continuing past the timeout is wrong with gorilla: a read timeout marks
	// the connection permanently failed, so the retry spins on an instant error
	// until the library panics.
	deadline := time.Now().Add(25 * time.Second)
	socket.SetReadDeadline(deadline)
	joinSent := false
	for {
		_, message, err := socket.ReadMessage()
		if err != nil {
			if strings.Contains(err.Error(), "timeout") {
				return "TIMEOUT"
			}
			return "ERROR:ws-closed:" + err.Error()
		}
		text := string(message)
		if text == "2" {
			socket.WriteMessage(websocket.TextMessage, []byte("3"))
			continue
		}
		if text == "40" && !joinSent {
			joinSent = true
			socket.WriteMessage(websocket.TextMessage, []byte(join))
			continue
		}
		if !joinSent || !strings.HasPrefix(text, "42") {
			continue
		}
		var frame []json.RawMessage
		if json.Unmarshal([]byte(text[2:]), &frame) != nil || len(frame) == 0 {
			continue
		}
		event := strings.Trim(string(frame[0]), `"`)
		if event == "5" {
			return "JOINED"
		}
		if event == "6" {
			reason := "null"
			if len(frame) > 1 {
				reason = string(frame[1])
			}
			return "REJECTED code=" + reason
		}
	}

}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("ERROR:no-token-argument")
		return
	}
	fmt.Println(run(os.Args[1]))
}
