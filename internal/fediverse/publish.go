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

package fediverse

import (
	"context"
	"sync"

	"github.com/Warp-net/activity-pub-gw/internal/warp"

	log "github.com/sirupsen/logrus"
)

// publishNote fans a Warpnet tweet out to every Fediverse follower of localUser
// as a signed Create(Note). Delivery is best-effort per follower and bounded by
// the gateway's delivery semaphore; it blocks until all deliveries settle.
func (g *Gateway) publishNote(ctx context.Context, localUser string, t warp.Tweet) {
	g.deliverToFollowers(ctx, localUser, g.buildCreateNote(localUser, t), "publish tweet "+t.Id)
}

// FederateTweetAsync fans a gossiped top-level owner tweet out to the author's
// Fediverse followers without blocking the libp2p gossip ack. The author's node
// pushes new posts here (PublishUpdateToFollowers over the private tweet route)
// the moment they are created, so this delivers in real time instead of waiting
// on the 60s poller — and is idempotent with it, since the Create/Note id is
// derived from the tweet id, so a duplicate delivery is ignored by the peer.
func (g *Gateway) FederateTweetAsync(t warp.Tweet) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), acceptDeliveryTimeout)
		defer cancel()
		g.publishNote(ctx, t.UserId, t)
	}()
}

// sendActorUpdate pushes an Update(Person) to localUser's Fediverse followers so
// their servers refresh the cached profile (avatar, bio, the Warpnet badge).
// Mastodon only re-fetches a remote actor on an Update or after its cache goes
// stale, so a profile change is otherwise invisible to existing followers.
func (g *Gateway) sendActorUpdate(ctx context.Context, localUser string) {
	wu, ok := g.source.GetUser(localUser)
	if !ok {
		return
	}
	actorID := g.ActorID(localUser)
	update := activity{
		Context: []any{asContext, secContext},
		ID:      actorID + "#update-" + warp.RandomToken(),
		Type:    typeUpdate,
		Actor:   actorID,
		Object:  g.buildActor(wu),
		To:      []string{asPublic},
		Cc:      []string{actorID + PathFollowers},
	}
	g.deliverToFollowers(ctx, localUser, update, "profile-update "+localUser)
}

// deliverToFollowers fans doc out to every Fediverse follower of localUser as a
// signed delivery, bounded by the delivery semaphore; label prefixes the logs.
func (g *Gateway) deliverToFollowers(ctx context.Context, localUser string, doc any, label string) {
	actorURLs, err := g.Followers.List(localUser)
	if err != nil {
		log.Errorf("%s: list followers of %s: %v", label, localUser, err)
		return
	}
	if len(actorURLs) == 0 {
		log.Infof("%s: %s has no Fediverse followers, skipping", label, localUser)
		return
	}

	var wg sync.WaitGroup
	for _, actorURL := range actorURLs {
		select {
		case g.sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return
		}
		wg.Add(1)
		go func(actor string) {
			defer wg.Done()
			defer func() { <-g.sem }()
			inbox, ierr := g.remoteInbox(ctx, actor)
			if ierr != nil {
				log.Errorf("%s: resolve inbox for %s: %v", label, actor, ierr)
				return
			}
			if perr := g.postSigned(ctx, localUser, inbox, doc); perr != nil {
				log.Errorf("%s: deliver to %s: %v", label, inbox, perr)
				return
			}
			log.Infof("%s: delivered to %s", label, inbox)
		}(actorURL)
	}
	wg.Wait()
}
