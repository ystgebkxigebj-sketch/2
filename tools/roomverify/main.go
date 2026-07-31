// roomverify — an HONEST self-test for a token producer running on a CI runner.
//
// ─────────────────────────────────────────────────────────────────────────────
// WHY THIS REPLACES `joinverify`
//
// The old verifier (repo 1, `inputs/joinverify/main.go`) could not report an
// acceptance even when the token was perfect, for three independent reasons:
//
//  1. IT JOINED PUBLIC MATCHMAKING. It discovered with
//     `/server/?check=1&v3=1&lang=19` and joined with `42[1,{…,"idioma":19}]`.
//     That is the `-lang N` path, and since 2026-07-30 ~22:46Z gartic answers it
//     with `42["6",1]` — code 1 — for EVERY language, on every exit, INCLUDING a
//     residential control that specific-room joins accept in the same minute.
//     Code 1 is a ROOM-STATE rejection that lands *before* the token is read.
//     So the verifier's verdict was decided before the thing it was measuring
//     was ever examined.
//
//  2. IT CLASSIFIED WITH A DENY-LIST. `case "$V" in JOINED*) accepted++` in the
//     workflow means `code 3` (room full) and `code 4` (already playing) — both
//     of which PROVE the token was accepted, because the token gate runs before
//     the capacity and dedup checks — were scored as refusals, and `code 1`,
//     Cloudflare 1015 and every dial error landed in the denominator.
//
//  3. IT USED A FIXED NICK ("probe") AND A SINGLE TARGET. gartic holds a
//     per-(IP,room) re-join lock of ~45-60 s, which manufactures `code 4` and,
//     on repeat, ghosts.
//
// This binary fixes all three: it dials a SPECIFIC, ROTATING room with event 3,
// randomises the nick, and emits a bucket from an ALLOW-LIST that a caller can
// count without interpretation.
//
// It also carries the room roster fetch (`-list-rooms`) so a runner has its own
// source and does not need the VM's loopback-only Bird.
//
// ─────────────────────────────────────────────────────────────────────────────
// BUCKETS — the only two that may enter an acceptance ratio are
// ACCEPTED_* (numerator+denominator) and REFUSED_token (denominator).
// Everything prefixed NOVERDICT_ must be excluded from BOTH.
//
//	ACCEPTED_joined          event 5           token accepted, we are in
//	ACCEPTED_roomfull        code 3            capacity check passed the token
//	ACCEPTED_alreadyplaying  code 4            dedup check passed the token
//	REFUSED_token            code 5            the ONLY token refusal
//	NOVERDICT_roomstate      code 1            room refused before reading the token
//	NOVERDICT_cf1015         429 / "code: 1015"  Cloudflare throttled the DIAL
//	NOVERDICT_discover       server lookup failed
//	NOVERDICT_dial           websocket handshake failed
//	NOVERDICT_timeout        no verdict frame within the deadline
//	NOVERDICT_other          anything else
//
// Output is exactly one line of JSON on stdout, always — including on error —
// so a caller can parse unconditionally.
//
// Usage:
//
//	roomverify [-proxy socks5://h:p] -room 4912AJ [-nick zz123] [-timeout 25] <token>
//	roomverify [-proxy socks5://h:p] -list-rooms 19,2,1,0,5 [-min-quant 0]
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/net/proxy"
)

const userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
	"(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

type dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// transportFor resolves an optional proxy spec into the two knobs both the HTTP
// client and the websocket dialer need. The split matters: a SOCKS proxy is a
// custom dialer, whereas an HTTP proxy must go through the standard Proxy hook
// so that a CONNECT is issued. Using a raw dialer for an http:// proxy would
// silently talk TLS to the proxy itself.
func transportFor(spec string) (dialFunc, func(*http.Request) (*url.URL, error), error) {
	base := &net.Dialer{Timeout: 15 * time.Second}
	if spec == "" {
		return base.DialContext, nil, nil
	}
	parsed, err := url.Parse(spec)
	if err != nil {
		return nil, nil, err
	}
	if strings.HasPrefix(parsed.Scheme, "socks") {
		var auth *proxy.Auth
		if parsed.User != nil {
			password, _ := parsed.User.Password()
			auth = &proxy.Auth{User: parsed.User.Username(), Password: password}
		}
		socks, err := proxy.SOCKS5("tcp", parsed.Host, auth, base)
		if err != nil {
			return nil, nil, err
		}
		return func(ctx context.Context, network, addr string) (net.Conn, error) {
			return socks.Dial(network, addr)
		}, nil, nil
	}
	return base.DialContext, http.ProxyURL(parsed), nil
}

type result struct {
	Bucket string `json:"bucket"`
	Code   string `json:"code,omitempty"`
	Room   string `json:"room,omitempty"`
	Nick   string `json:"nick,omitempty"`
	Ms     int64  `json:"ms"`
	Detail string `json:"detail,omitempty"`
}

func emit(r result) {
	line, _ := json.Marshal(r)
	fmt.Println(string(line))
}

// cf1015 is checked before anything else on every failure path. Cloudflare's
// per-redeeming-address WS throttle yields NO verdict, and every instrument this
// project had before 2026-07-30 hid it inside a generic bucket, where it is a
// near-perfect counterfeit of "acceptance collapsed when we scaled".
func looksLike1015(status int, body string) bool {
	return status == 429 || strings.Contains(body, "error code: 1015") ||
		strings.Contains(body, "1015")
}

func newClient(dial dialFunc, hook func(*http.Request) (*url.URL, error)) *http.Client {
	return &http.Client{
		Timeout: 25 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			DialContext:     dial,
			Proxy:           hook,
		},
	}
}

func get(client *http.Client, target string) (int, string, http.Header, error) {
	request, err := http.NewRequest("GET", target, nil)
	if err != nil {
		return 0, "", nil, err
	}
	request.Header.Set("User-Agent", userAgent)
	request.Header.Set("Origin", "https://gartic.io")
	request.Header.Set("Referer", "https://gartic.io/")
	request.Header.Set("Accept", "*/*")
	response, err := client.Do(request)
	if err != nil {
		return 0, "", nil, err
	}
	raw, _ := io.ReadAll(response.Body)
	response.Body.Close()
	return response.StatusCode, string(raw), response.Header, nil
}

// listRooms prints one room code per line, pulled from gartic's own public room
// listing. This is what makes a runner self-sufficient: the VM's Bird roster
// (http://127.0.0.1:8082/api/rooms) is loopback-only and unreachable from CI.
//
// `search=` empty returns only rooms with ACTIVE players, sorted by rating and
// capped around 100 — exactly the churn we want, and it means a rotating dial
// almost never revisits a room inside the ~45-60 s per-(IP,room) re-join lock.
//
// The endpoint is behind Cloudflare and answers 403 to a bare curl: the
// browser-shaped headers above are load-bearing, not decoration.
func listRooms(client *http.Client, langs string, minQuant int, fullOnly bool) int {
	type room struct {
		Code  string `json:"code"`
		Quant int    `json:"quant"`
		Max   int    `json:"max"`
	}
	seen := map[string]bool{}
	var out []string
	var lastErr string
	for _, lang := range strings.Split(langs, ",") {
		lang = strings.TrimSpace(lang)
		if lang == "" {
			continue
		}
		status, body, _, err := get(client,
			"https://gartic.io/req/list?search=&language%5B%5D="+url.QueryEscape(lang))
		if err != nil {
			lastErr = err.Error()
			continue
		}
		if status != 200 {
			lastErr = fmt.Sprintf("status=%d", status)
			continue
		}
		var rooms []room
		if json.Unmarshal([]byte(body), &rooms) != nil {
			lastErr = "unparseable"
			continue
		}
		for _, r := range rooms {
			// A FULL room is the BEST target, not merely an acceptable one:
			// `code 3` proves the token was accepted (the Turnstile gate runs
			// before the capacity check) and the join adds nobody to the room, so
			// a verifier that dials only full rooms measures acceptance without
			// ever putting a ghost player into somebody's game. -full asks for
			// exactly those; without it every listed room is fair game.
			if r.Code == "" || seen[r.Code] || r.Quant < minQuant {
				continue
			}
			if fullOnly && (r.Max <= 0 || r.Quant < r.Max) {
				continue
			}
			seen[r.Code] = true
			out = append(out, r.Code)
		}
	}
	if len(out) == 0 {
		fmt.Fprintf(os.Stderr, "roomverify: no rooms (%s)\n", lastErr)
		return 1
	}
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	for _, c := range out {
		fmt.Println(c)
	}
	return 0
}

func verify(client *http.Client, dial dialFunc, hook func(*http.Request) (*url.URL, error),
	token, room, nick string, timeout time.Duration) result {
	started := time.Now()
	ms := func() int64 { return time.Since(started).Milliseconds() }

	// ── DISCOVER. Specific-room resolution, NOT `&lang=N` matchmaking. This one
	// character of difference is the whole defect the old verifier had.
	status, body, header, err := get(client,
		"https://gartic.io/server/?check=1&v3=1&room="+url.QueryEscape(room))
	if err != nil {
		if looksLike1015(0, err.Error()) {
			return result{Bucket: "NOVERDICT_cf1015", Room: room, Ms: ms(), Detail: err.Error()}
		}
		return result{Bucket: "NOVERDICT_discover", Room: room, Ms: ms(), Detail: err.Error()}
	}
	if looksLike1015(status, body) {
		return result{Bucket: "NOVERDICT_cf1015", Room: room, Ms: ms(),
			Detail: fmt.Sprintf("discover status=%d", status)}
	}
	if !strings.Contains(body, "?c=") || !strings.Contains(body, "https://") {
		snippet := strings.Map(func(r rune) rune {
			if r == '\n' || r == '\r' || r == '\t' {
				return ' '
			}
			return r
		}, body)
		if len(snippet) > 120 {
			snippet = snippet[:120]
		}
		return result{Bucket: "NOVERDICT_discover", Room: room, Ms: ms(),
			Detail: fmt.Sprintf("status=%d body=%q", status, snippet)}
	}
	server := strings.Split(strings.Split(body, "https://")[1], ".")[0]
	code := strings.TrimSpace(strings.Split(body, "?c=")[1])

	var cookieParts []string
	for _, raw := range header.Values("Set-Cookie") {
		pair := strings.Split(raw, ";")[0]
		if pair != "" && !strings.HasSuffix(pair, "=") {
			cookieParts = append(cookieParts, pair)
		}
	}

	target := fmt.Sprintf("wss://%s.gartic.io/socket.io/?c=%s&EIO=3&transport=websocket",
		server, code)
	head := http.Header{}
	head.Set("User-Agent", userAgent)
	head.Set("Origin", "https://gartic.io")
	head.Set("Referer", "https://gartic.io/")
	if len(cookieParts) > 0 {
		head.Set("Cookie", strings.Join(cookieParts, "; "))
	}

	dialer := websocket.Dialer{
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: true},
		HandshakeTimeout: 15 * time.Second,
		NetDialContext:   dial,
		Proxy:            hook,
	}
	socket, dialResponse, err := dialer.Dial(target, head)
	if err != nil {
		st := 0
		if dialResponse != nil {
			st = dialResponse.StatusCode
		}
		if looksLike1015(st, err.Error()) {
			return result{Bucket: "NOVERDICT_cf1015", Room: room, Ms: ms(),
				Detail: fmt.Sprintf("ws status=%d", st)}
		}
		return result{Bucket: "NOVERDICT_dial", Room: room, Ms: ms(),
			Detail: fmt.Sprintf("status=%d %v", st, err)}
	}
	defer socket.Close()

	// A SPECIFIC-ROOM join is event 3 with "sala", and sala is the room code with
	// its two-character language prefix removed. (Event 1 + "idioma" is the
	// public-matchmaking join — the one that now always answers code 1.)
	sala := room
	if len(room) > 2 {
		sala = room[2:]
	}
	join := fmt.Sprintf(`42[3,{"v":20000,"token":"%s","nick":"%s","avatar":0,"platform":0,"sala":"%s"}]`,
		token, nick, sala)

	// One deadline for the whole exchange. Re-arming a short deadline and
	// continuing past the timeout is wrong with gorilla: a read timeout marks the
	// connection permanently failed, so the retry spins on an instant error.
	socket.SetReadDeadline(time.Now().Add(timeout))
	joinSent := false
	for {
		_, message, err := socket.ReadMessage()
		if err != nil {
			if strings.Contains(err.Error(), "timeout") {
				return result{Bucket: "NOVERDICT_timeout", Room: room, Nick: nick, Ms: ms()}
			}
			return result{Bucket: "NOVERDICT_other", Room: room, Nick: nick, Ms: ms(),
				Detail: "ws-closed:" + err.Error()}
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
			return result{Bucket: "ACCEPTED_joined", Code: "0", Room: room, Nick: nick, Ms: ms()}
		}
		if event == "6" {
			reason := "null"
			if len(frame) > 1 {
				reason = strings.Trim(string(frame[1]), `"`)
			}
			// ⚠️ ALLOW-LIST. code 3 and code 4 mean the token was ACCEPTED: the
			// Turnstile gate runs BEFORE both the capacity check and the
			// already-playing dedup check, so reaching either of them is proof the
			// token passed. Only code 5 is a token refusal.
			switch reason {
			case "3":
				return result{Bucket: "ACCEPTED_roomfull", Code: reason, Room: room, Nick: nick, Ms: ms()}
			case "4":
				return result{Bucket: "ACCEPTED_alreadyplaying", Code: reason, Room: room, Nick: nick, Ms: ms()}
			case "5":
				return result{Bucket: "REFUSED_token", Code: reason, Room: room, Nick: nick, Ms: ms()}
			case "1":
				return result{Bucket: "NOVERDICT_roomstate", Code: reason, Room: room, Nick: nick, Ms: ms()}
			case "6":
				// The room stopped existing between the listing and the dial.
				return result{Bucket: "NOVERDICT_roomstate", Code: reason, Room: room, Nick: nick, Ms: ms()}
			default:
				return result{Bucket: "NOVERDICT_other", Code: reason, Room: room, Nick: nick, Ms: ms()}
			}
		}
	}
}

func main() {
	proxySpec := flag.String("proxy", "", "socks5://host:port to send the check through (default: direct)")
	room := flag.String("room", "", "specific room code to dial, e.g. 4912AJ")
	nick := flag.String("nick", "", "nick to join with (default: random)")
	listLangs := flag.String("list-rooms", "", "print room codes for these comma-separated language ids and exit")
	minQuant := flag.Int("min-quant", 0, "when listing, drop rooms with fewer than this many players")
	fullOnly := flag.Bool("full", false, "when listing, keep only FULL rooms (quant >= max) — a full room answers code 3, which proves acceptance and adds nobody to the room")
	timeout := flag.Int("timeout", 25, "seconds to wait for a verdict frame")
	flag.Parse()

	dial, hook, err := transportFor(*proxySpec)
	if err != nil {
		emit(result{Bucket: "NOVERDICT_other", Detail: "proxy:" + err.Error()})
		return
	}
	client := newClient(dial, hook)

	if *listLangs != "" {
		os.Exit(listRooms(client, *listLangs, *minQuant, *fullOnly))
	}
	if flag.NArg() < 1 || flag.Arg(0) == "" {
		emit(result{Bucket: "NOVERDICT_other", Detail: "no-token-argument"})
		return
	}
	if *room == "" {
		emit(result{Bucket: "NOVERDICT_other", Detail: "no-room"})
		return
	}
	if *nick == "" {
		*nick = "zz" + strconv.Itoa(100000+rand.Intn(899999))
	}
	emit(verify(client, dial, hook, flag.Arg(0), *room, *nick, time.Duration(*timeout)*time.Second))
}
