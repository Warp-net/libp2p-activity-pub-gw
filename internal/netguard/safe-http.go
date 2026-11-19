/*

 Warpnet - Decentralized Social Network
 Copyright (C) 2025 Vadim Filin, https://github.com/Warp-net,
 <github.com.mecdy@passmail.net>

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <https://www.gnu.org/licenses/>.

WarpNet is provided “as is” without warranty of any kind, either expressed or implied.
Use at your own risk. The maintainers shall not be liable for any damages or data loss
resulting from the use or misuse of this software.
*/

// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package netguard

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// ValidateURL is the SSRF guard for dereferencing attacker-supplied
// actor/key URLs: it requires https, a host, and rejects localhost plus literal
// IPs in loopback/private/link-local/unspecified ranges.
// TODO(prod): also guard hostnames that resolve into those ranges (DNS
// rebinding) at dial time before exposing the gateway publicly.
func ValidateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse url %q: %w", raw, err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("url %q: %w", raw, ErrInsecureURL)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("url %q has no host: %w", raw, ErrBlockedHost)
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return fmt.Errorf("url %q targets localhost: %w", raw, ErrBlockedHost)
	}
	if addr, perr := netip.ParseAddr(host); perr == nil && IsBlockedIP(addr) {
		return fmt.Errorf("url %q targets a disallowed address: %w", raw, ErrBlockedHost)
	}
	return nil
}

// IsBlockedIP reports whether ip is in a range outbound federation must never
// reach (loopback, private, link-local, multicast, unspecified).
func IsBlockedIP(ip netip.Addr) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast()
}

// NewClient builds the HTTP client for outbound federation, hardened against
// SSRF on attacker-supplied actor/key URLs: CheckRedirect re-applies the URL
// guard to every redirect hop (and caps the chain), and the dialer's Control
// validates the *resolved* IP at connect time, closing DNS-rebinding that the
// hostname checks can't see. Signatures aren't re-applied across redirects, so a
// redirected signed fetch fails at the target — failing closed.
func NewClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	dialer.Control = func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}
		if ip, perr := netip.ParseAddr(host); perr == nil && IsBlockedIP(ip) {
			return fmt.Errorf("dial %s: %w", address, ErrBlockedHost)
		}
		return nil
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = dialer.DialContext
	// A federation gateway talks to a handful of instances but fans many fetches
	// at each (a thread pulls the note, its stats, every reply and their authors).
	// The stdlib default keeps only 2 idle connections per host, so concurrent
	// fetches force fresh TLS handshakes every batch; pool enough to reuse them.
	tr.MaxIdleConnsPerHost = 32
	return &http.Client{
		Timeout:   timeout,
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= MaxRedirects {
				return errTooManyRedirects
			}
			return ValidateURL(req.URL.String())
		},
	}
}

// MaxRedirects caps redirect hops for outbound federation fetches.
const MaxRedirects = 5

var (
	ErrInsecureURL      = errors.New("remote URL must be https")
	ErrBlockedHost      = errors.New("remote URL host is not allowed")
	errTooManyRedirects = errors.New("too many redirects")
)
