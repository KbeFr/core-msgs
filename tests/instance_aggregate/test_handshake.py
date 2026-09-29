"""Validation for handshake.py: one section per flow, then a randomized fuzz.

The aggregate is simulated the way the real one would use it: ONE dict of handshake
objects per topic, no router, `hs.handle(env)` / `hs.tick()` / `hs.done`.
"""
from __future__ import annotations

import random
from collections import deque
from dataclasses import dataclass

import pytest

from core_msgs.instance_aggregate.handshake_shared import (
    HandshakeEnvelope, HandshakeStatus as HS, InitiatorState as IS)
from core_msgs.instance_aggregate.handshake import (
    HandshakeAction as A, HandshakeAuction, HandshakeInitiator, HandshakeResponder,
    HandshakeResult, Reservation, SessionState as SS, orphan_reply)

AGG = "agg"


class Clock:
    def __init__(self): self.t = 0.0
    def __call__(self): return self.t
    def advance(self, dt): self.t += dt


@dataclass
class Bid:
    time_bidding: float


def lowest_time(payload, bids, hints):
    ok = {i: b for i, b in bids.items() if b is not None}
    return min(ok, key=lambda i: ok[i].time_bidding) if ok else None


class FakeAggregate:
    """Exactly the shape the real AggregateTwin uses: one dict, duck-typed handshakes."""

    def __init__(self, clock, timeout=2.0):
        self.name = AGG
        self.clock = clock
        self.timeout = timeout
        self.handshakes: dict[str, object] = {}
        self.links, self.releases, self.completes = [], [], []
        self.outbox: list = []

    # -- starting one --
    def request(self, subject, payload=None, target=None):
        if subject in self.handshakes:
            return
        hs = HandshakeInitiator(subject, self.name, timeout=self.timeout, clock=self.clock)
        self.handshakes[subject] = hs
        self.outbox.append(hs.request(payload=payload, target=target))

    def auction(self, subject, hints, get_winner_fn, payload=None):
        if subject in self.handshakes:
            return
        hs = HandshakeAuction(subject, self.name, hints, get_winner_fn, payload=payload,
                              timeout=self.timeout, clock=self.clock)
        self.handshakes[subject] = hs
        self._apply(hs.open())

    def cancel(self, subject, reason="cancelled by aggregate"):
        hs = self.handshakes.get(subject)
        if hs is not None:
            self._apply(hs.cancel(reason))

    # -- inbound / tick --
    def route(self, env):
        hs = self.handshakes.get(env.id)
        if hs is None:
            reply = orphan_reply(env, self.name)
            if reply is not None:
                self.outbox.append(reply)
            return
        self._apply(hs.handle(env))
        self._retire(env.id, hs)

    def tick(self):
        for subject, hs in list(self.handshakes.items()):
            self._apply(hs.tick())
            self._retire(subject, hs)

    def _retire(self, subject, hs):
        if hs.done:
            self.handshakes.pop(subject, None)

    def _apply(self, res: HandshakeResult):
        self.outbox.extend(e for e in res.out if e is not None)
        if res.action is A.LINK_SUBJECT:
            self.links.append((res.subject, res.receiver))
        elif res.action is A.RELEASE_SUBJECT:
            self.releases.append((res.subject, res.receiver))
        elif res.action is A.COMPLETE_SUBJECT:
            self.completes.append((res.subject, res.receiver))


class Net:
    def __init__(self, agg, responders, rng=None, drop=0.0):
        self.agg, self.resp, self.rng, self.drop = agg, responders, rng, drop
        self.q = deque()
        self.actions = []
        self.log = []

    def pump(self):
        for env in self.agg.outbox:
            self.q.append(("inst", env))
        self.agg.outbox.clear()

    def from_instance(self, env):
        if env is not None:
            self.q.append(("agg", env))

    def run(self, limit=10_000):
        n = 0
        self.pump()
        while self.q:
            n += 1
            assert n < limit, "message storm"
            i = self.rng.randrange(len(self.q)) if self.rng else 0
            self.q.rotate(-i)
            side, env = self.q.popleft()
            if self.drop and self.rng.random() < self.drop:
                continue
            self.log.append((side, env.handshake_status.value, env.id, env.sender, env.target))
            if side == "inst":
                for name in (list(self.resp) if env.target is None else [env.target]):
                    if name not in self.resp:
                        continue
                    r = self.resp[name].handle(env)
                    if r.outbound is not A.DO_NOTHING:
                        self.actions.append((name, r.outbound, r.subject))
                    self.from_instance(r.reply)
            else:
                self.agg.route(env)
                self.pump()


def world(n=3, **rkw):
    clk = Clock()
    agg = FakeAggregate(clk)
    resp = {f"i{k}": HandshakeResponder(f"i{k}", clock=clk, **rkw) for k in range(1, n + 1)}
    return clk, agg, resp, Net(agg, resp)


# ══ POOLED: global topic, first reply wins ═════════════════════════════════

def test_pooled_first_ack_wins_and_the_rest_are_cancelled():
    clk, agg, resp, net = world(3, max_reserved=1)
    agg.request("agentA", payload="disc")                      # no target, no hint
    net.run()
    assert agg.links == [("agentA", "i1")]
    assert net.actions == [("i1", A.LINK_SUBJECT, "agentA")]
    assert resp["i1"].active == "agentA"
    assert all(not r.reserved for r in resp.values()), "a loser kept a reservation"
    assert [l[1] for l in net.log].count("bid") == 0, "pooled must not bid"


def test_pooled_release_frees_the_instance_and_drops_the_handshake():
    clk, agg, resp, net = world(2, max_reserved=1)
    agg.request("agentA", payload={"kind": "ugv"})
    net.run()
    winner = agg.links[0][1]
    assert resp[winner].active_env.payload == {"kind": "ugv"}
    agg.cancel("agentA")
    net.run()
    assert agg.releases == [("agentA", winner)]
    assert resp[winner].active is None and "agentA" not in agg.handshakes


def test_pooled_instance_does_not_promise_itself_to_two_agents():
    """What the old `confirmed` flag protected."""
    clk, agg, resp, net = world(1, max_reserved=1)
    agg.request("A", payload=1)
    agg.request("B", payload=2)
    net.run()
    assert resp["i1"].active == "A" and not resp["i1"].reserved
    assert [s for s, _ in agg.links] == ["A"]


def test_pooled_late_ack_is_cancelled_not_linked():
    clk, agg, resp, net = world(2, max_reserved=1)
    agg.request("A", payload=1)
    req = agg.outbox[0]
    net.run()
    assert resp["i1"].active == "A"
    net.from_instance(resp["i2"].handle(req).reply)             # i2 wakes up late
    net.run()
    assert resp["i2"].active is None and not resp["i2"].reserved
    assert len(agg.links) == 1


def test_pooled_nobody_answers_times_out_and_can_be_retried():
    clk, agg, resp, net = world(0)
    agg.request("A", payload=1)
    net.run()
    clk.advance(2.5)
    agg.tick()
    assert "A" not in agg.handshakes and agg.links == []
    agg.request("A", payload=1)                                 # next discovery tick retries
    assert "A" in agg.handshakes


# ══ DIRECTED: aggregate knows who, no bidding ══════════════════════════════

def test_directed_acks_and_links_without_bidding():
    clk, agg, resp, net = world(3, max_reserved=1)
    agg.request("agentA", payload="disc", target="i2")
    net.run()
    assert agg.links == [("agentA", "i2")]
    assert resp["i2"].active == "agentA"
    assert resp["i1"].active is None and not resp["i1"].reserved
    assert [l[1] for l in net.log].count("bid") == 0


def test_directed_to_a_busy_instance_is_refused():
    clk, agg, resp, net = world(1)
    agg.request("m1", payload="mission", target="i1")
    net.run()
    agg.handshakes.pop("m1")                                    # pretend a second mission
    agg.request("m2", payload="mission", target="i1")
    net.run()
    assert resp["i1"].active == "m1"
    assert [s for s, _ in agg.links] == ["m1"]
    assert "m2" not in agg.handshakes                           # refused -> dropped, retryable


def test_directed_bind_failure_revokes_and_releases():
    clk, agg, resp, net = world(1)
    agg.request("agentA", payload=1, target="i1")
    net.run()
    assert agg.links == [("agentA", "i1")]
    net.from_instance(resp["i1"].get_revoked("agentA"))         # _bind raised in the twin
    net.run()
    assert agg.releases == [("agentA", "i1")]
    assert resp["i1"].active is None and "agentA" not in agg.handshakes


def test_adding_a_hint_switches_the_same_call_to_bidding():
    clk, agg, resp, net = world(1, bid_fn=lambda h: Bid(h["t"]))
    initiator = HandshakeInitiator("m1", AGG, receiver="i1", clock=clk)
    env = initiator.request(payload="mission", hint={"t": 5})
    reply = resp["i1"].handle(env).reply
    assert reply.handshake_status is HS.BID and reply.bid.time_bidding == 5
    assert initiator.bidding is True


# ══ AUCTION ════════════════════════════════════════════════════════════════

def test_auction_lowest_bid_wins_losers_released():
    clk, agg, resp, net = world(3, bid_fn=lambda h: Bid(h["t"]))
    agg.auction("m1", {"i1": {"t": 30}, "i2": {"t": 10}, "i3": {"t": 20}},
                lowest_time, payload="mission")
    net.run()
    assert agg.links == [("m1", "i2")]
    assert resp["i2"].active == "m1"
    assert all(not r.reserved for r in resp.values())
    assert agg.handshakes["m1"].receiver == "i2"        # same object still owns the subject


def test_auction_waits_for_every_bid():
    clk, agg, resp, net = world(2, bid_fn=lambda h: Bid(h["t"]))
    agg.auction("m1", {"i1": {"t": 5}, "i2": {"t": 1}}, lowest_time)
    first = [e for e in agg.outbox if e.target == "i1"]
    second = [e for e in agg.outbox if e.target == "i2"]
    agg.outbox = first
    net.run()
    assert agg.handshakes["m1"].state is SS.SOLICITING
    agg.outbox = second
    net.run()
    assert agg.links == [("m1", "i2")]


def test_auction_deadline_awards_among_the_instances_that_answered():
    clk, agg, resp, net = world(3, bid_fn=lambda h: Bid(h["t"]))
    agg.auction("m1", {"i1": {"t": 9}, "i2": {"t": 3}, "i3": {"t": 1}}, lowest_time)
    deaf = [e for e in agg.outbox if e.target == "i3"]
    agg.outbox = [e for e in agg.outbox if e.target != "i3"]
    net.run()
    assert agg.handshakes["m1"].state is SS.SOLICITING
    clk.advance(2.5)
    agg.tick(); net.run()
    assert agg.links == [("m1", "i2")]
    agg.outbox = deaf; net.run()                                # i3 finally hears it
    assert resp["i3"].active is None and not resp["i3"].reserved
    assert len(agg.links) == 1


def test_auction_all_refuse_fails_and_is_dropped_for_replanning():
    clk, agg, resp, net = world(2, bid_fn=lambda h: None)
    agg.auction("m1", {"i1": {}, "i2": {}}, lowest_time)
    net.run()
    assert agg.links == [] and "m1" not in agg.handshakes


def test_auction_winner_never_confirms_is_cancelled_not_left_holding():
    """Bug in mission_handshake_1: _stop() skipped the timed-out AWARDED winner, so the
    instance kept a mission the aggregate had already given up on."""
    clk = Clock()
    resp = HandshakeResponder("i1", clock=clk, bid_fn=lambda h: Bid(1))
    auction = HandshakeAuction("m1", AGG, {"i1": {}}, lowest_time, timeout=2.0, clock=clk)

    bid = resp.handle(auction.open().out[0]).reply
    award = auction.handle(bid).out[0]
    assert award.handshake_status is HS.ACK
    assert resp.handle(award).action is A.LINK_SUBJECT           # it linked...
    assert auction.state is SS.AWARDING                          # ...but the confirm was lost

    clk.advance(2.5)
    out = auction.tick().out
    assert [e.handshake_status for e in out] == [HS.CANCEL]
    assert resp.handle(out[0]).action is A.RELEASE_SUBJECT
    assert resp.active is None and auction.state is SS.FAILED


def test_auction_winner_revokes_after_going_active():
    clk, agg, resp, net = world(2, bid_fn=lambda h: Bid(1))
    agg.auction("m1", {"i1": {}, "i2": {}}, lambda p, b, h: "i1")
    net.run()
    assert agg.links == [("m1", "i1")]
    net.from_instance(resp["i1"].get_revoked("m1")); net.run()
    assert agg.releases == [("m1", "i1")] and "m1" not in agg.handshakes


def test_auction_cancel_mid_bidding_releases_reservations():
    clk, agg, resp, net = world(2, bid_fn=lambda h: Bid(1))
    agg.auction("m1", {"i1": {}, "i2": {}}, lowest_time)
    late = [e for e in agg.outbox if e.target == "i2"]
    agg.outbox = [e for e in agg.outbox if e.target == "i1"]
    net.run()
    assert resp["i1"].reserved
    agg.cancel("m1"); net.run()
    assert not resp["i1"].reserved
    agg.outbox = late; net.run()
    assert not resp["i2"].reserved


def test_auction_get_winner_exception_fails_cleanly():
    clk, agg, resp, net = world(1, bid_fn=lambda h: Bid(1))
    agg.auction("m1", {"i1": {}}, lambda p, b, h: 1 / 0)
    net.run()
    assert agg.links == [] and not resp["i1"].reserved


# ══ after linking: identical whatever assigned it ══════════════════════════

@pytest.mark.parametrize("mode", ["pooled", "directed", "auction"])
def test_complete_is_the_same_for_every_mode(mode):
    clk, agg, resp, net = world(1, bid_fn=lambda h: Bid(1))
    if mode == "pooled":
        agg.request("m1", payload="mission")
    elif mode == "directed":
        agg.request("m1", payload="mission", target="i1")
    else:
        agg.auction("m1", {"i1": {"t": 1}}, lowest_time, payload="mission")
    net.run()
    assert agg.links == [("m1", "i1")]

    net.from_instance(resp["i1"].get_completed("m1")); net.run()
    assert agg.completes == [("m1", "i1")] and agg.releases == []
    assert resp["i1"].active is None
    assert "m1" not in agg.handshakes


@pytest.mark.parametrize("mode", ["pooled", "directed", "auction"])
def test_cancel_after_linking_is_the_same_for_every_mode(mode):
    clk, agg, resp, net = world(1, bid_fn=lambda h: Bid(1))
    if mode == "pooled":
        agg.request("m1", payload="mission")
    elif mode == "directed":
        agg.request("m1", payload="mission", target="i1")
    else:
        agg.auction("m1", {"i1": {"t": 1}}, lowest_time, payload="mission")
    net.run()
    agg.cancel("m1"); net.run()
    assert agg.releases == [("m1", "i1")] and agg.completes == []
    assert resp["i1"].active is None and "m1" not in agg.handshakes


def test_release_that_is_never_acknowledged_is_given_up_on():
    clk, agg, resp, net = world(1)
    agg.request("A", payload=1, target="i1"); net.run()
    agg.cancel("A")
    agg.outbox.clear()                                          # every CANCEL is lost
    for _ in range(5):
        clk.advance(2.5)
        agg.tick()
        agg.outbox.clear()
    assert agg.releases == [("A", "i1")] and "A" not in agg.handshakes


def test_lost_complete_ack_is_repaired_by_the_orphan_rule():
    clk, agg, resp, net = world(1)
    agg.request("m1", payload=1, target="i1"); net.run()
    net.from_instance(resp["i1"].get_completed("m1")); net.run()
    assert "m1" not in agg.handshakes
    resp["i1"].active, resp["i1"].active_epoch = "m1", 1        # its ack was lost, retransmit
    net.from_instance(resp["i1"].get_completed("m1")); net.run()
    assert resp["i1"].active is None


def test_orphan_bid_and_orphan_claim_are_cancelled():
    for status in (HS.BID, HS.ACK):
        reply = orphan_reply(HandshakeEnvelope("ghost", status, "i1", epoch=1), AGG)
        assert reply.handshake_status is HS.CANCEL and reply.target == "i1"
    assert orphan_reply(HandshakeEnvelope("ghost", HS.CANCEL_ACK, "i1"), AGG) is None


def test_request_is_a_no_op_while_the_subject_is_in_flight():
    clk, agg, resp, net = world(1)
    agg.request("A", payload=1, target="i1")
    assert len(agg.outbox) == 1
    agg.request("A", payload=1, target="i1")                    # discovery repeats
    assert len(agg.outbox) == 1
    net.run()
    agg.request("A", payload=1, target="i1")                    # bound
    assert agg.outbox == []


# ══ responder / initiator units ════════════════════════════════════════════

def test_responder_ignores_other_targets_and_its_own_echo():
    r = HandshakeResponder("i1")
    assert r.handle(HandshakeEnvelope("A", HS.REQUEST, AGG, target="i2")).reply is None
    assert r.handle(HandshakeEnvelope("A", HS.REQUEST, "i1")).reply is None


def test_responder_reservation_ttl():
    clk = Clock()
    r = HandshakeResponder("i1", clock=clk, timeout=5.0, max_reserved=1)
    assert r.handle(HandshakeEnvelope("A", HS.REQUEST, AGG)).reply.handshake_status is HS.ACK
    assert r.handle(HandshakeEnvelope("B", HS.REQUEST, AGG)).reply.handshake_status is HS.NACK
    clk.advance(6)
    assert r.handle(HandshakeEnvelope("B", HS.REQUEST, AGG)).reply.handshake_status is HS.ACK


def test_responder_repeats_the_same_answer_on_a_retransmit():
    r = HandshakeResponder("i1", bid_fn=lambda h: Bid(1))
    req = HandshakeEnvelope("m", HS.REQUEST, AGG, epoch=1, hint={})
    assert r.handle(req).reply.handshake_status is HS.BID
    assert r.handle(req).reply.handshake_status is HS.BID
    assert len(r.reserved) == 1


def test_responder_duplicate_award_reconfirms_without_relinking():
    r = HandshakeResponder("i1")
    r.handle(HandshakeEnvelope("A", HS.REQUEST, AGG, epoch=1))
    award = HandshakeEnvelope("A", HS.ACK, AGG, epoch=1)
    assert r.handle(award).action is A.LINK_SUBJECT
    again = r.handle(award)
    assert again.action is A.DO_NOTHING and again.reply.handshake_status is HS.ACK


def test_responder_award_from_an_older_attempt_is_refused():
    r = HandshakeResponder("i1")
    r.handle(HandshakeEnvelope("A", HS.REQUEST, AGG, epoch=1))
    r.handle(HandshakeEnvelope("A", HS.REQUEST, AGG, epoch=2))
    stale = r.handle(HandshakeEnvelope("A", HS.ACK, AGG, epoch=1))
    assert stale.action is A.DO_NOTHING and stale.reply.handshake_status is HS.NACK
    assert r.handle(HandshakeEnvelope("A", HS.ACK, AGG, epoch=2)).action is A.LINK_SUBJECT


def test_responder_award_without_a_reservation_is_nacked():
    r = HandshakeResponder("i1")
    assert r.handle(HandshakeEnvelope("A", HS.ACK, AGG, epoch=1)).reply.handshake_status is HS.NACK


def test_responder_never_claims_a_second_subject():
    clk = Clock()
    r = HandshakeResponder("i1", clock=clk)
    r.handle(HandshakeEnvelope("A", HS.REQUEST, AGG, epoch=1))
    r.handle(HandshakeEnvelope("A", HS.ACK, AGG, epoch=1))
    r.reserved["B"] = Reservation(env=HandshakeEnvelope("B", HS.REQUEST, AGG, epoch=1), at=clk())
    res = r.handle(HandshakeEnvelope("B", HS.ACK, AGG, epoch=1))
    assert res.action is A.DO_NOTHING and res.reply.handshake_status is HS.NACK
    assert r.active == "A"


def test_responder_stale_cancel_does_not_kill_a_newer_attempt():
    r = HandshakeResponder("i1")
    r.handle(HandshakeEnvelope("A", HS.REQUEST, AGG, epoch=2))
    r.handle(HandshakeEnvelope("A", HS.ACK, AGG, epoch=2))
    assert r.handle(HandshakeEnvelope("A", HS.CANCEL, AGG, epoch=1)).action is A.DO_NOTHING
    assert r.active == "A"
    assert r.handle(HandshakeEnvelope("A", HS.CANCEL, AGG, epoch=2)).action is A.RELEASE_SUBJECT


def test_responder_re_request_for_what_we_hold_is_accepted():
    r = HandshakeResponder("i1")
    r.handle(HandshakeEnvelope("A", HS.REQUEST, AGG, epoch=1))
    r.handle(HandshakeEnvelope("A", HS.ACK, AGG, epoch=1))
    again = r.handle(HandshakeEnvelope("A", HS.REQUEST, AGG, epoch=2))
    assert again.reply.handshake_status is HS.ACK and r.active == "A"


def test_responder_hint_without_bid_fn_refuses():
    r = HandshakeResponder("i1")
    rep = r.handle(HandshakeEnvelope("m", HS.REQUEST, AGG, hint={})).reply
    assert rep.handshake_status is HS.NACK


def test_initiator_subject_required_returns_none():
    initiator = HandshakeInitiator(None, AGG)
    assert initiator.request(payload=1) is None
    initiator.bind_subject("A")
    assert initiator.request(payload=1) is not None


def test_initiator_stale_epoch_and_illegal_are_dropped():
    initiator = HandshakeInitiator("A", AGG, receiver="i1")
    initiator.request()
    assert initiator.handle(HandshakeEnvelope("A", HS.BID, "i1", epoch=99)).state is None
    assert initiator.handle(HandshakeEnvelope("A", HS.COMPLETE, "i1", epoch=1)).state is None
    assert initiator.illegal == 1


def test_initiator_bind_confirmation_is_not_illegal():
    initiator = HandshakeInitiator("A", AGG)
    initiator.request()
    res = initiator.handle(HandshakeEnvelope("A", HS.ACK, "i1", epoch=1))
    assert res.action is A.LINK_SUBJECT and res.out[0].handshake_status is HS.ACK
    again = initiator.handle(HandshakeEnvelope("A", HS.ACK, "i1", epoch=1))
    assert again.action is A.DO_NOTHING and initiator.illegal == 0


def test_bidding_request_does_not_link_on_a_bare_ack():
    """In a bidding conversation the award goes through the auction, so an ACK while
    REQUESTED must not link behind its back."""
    initiator = HandshakeInitiator("m1", AGG, receiver="i1")
    initiator.request(payload="mission", hint={"t": 1})
    res = initiator.handle(HandshakeEnvelope("m1", HS.ACK, "i1", epoch=1))
    assert res.action is A.DO_NOTHING and initiator.illegal == 1
    assert initiator.state is IS.REQUESTED


# ══ fuzz ═══════════════════════════════════════════════════════════════════

def invariants(agg, resp):
    holders = {}
    for name, r in resp.items():
        if r.active:
            holders.setdefault(r.active, []).append(name)
    for subject, names in holders.items():
        assert len(names) == 1, f"{subject} held by {names}"
    for subject, hs in agg.handshakes.items():
        if isinstance(hs, HandshakeAuction) and hs.state is SS.ACTIVE:
            assert resp[hs.winner].active == subject


@pytest.mark.parametrize("seed", range(60))
def test_fuzz_mixed_modes_random_order(seed):
    rng = random.Random(seed)
    clk = Clock()
    agg = FakeAggregate(clk)
    resp = {f"i{k}": HandshakeResponder(f"i{k}", clock=clk,
                                        bid_fn=lambda h: Bid(h["t"])) for k in range(4)}
    net = Net(agg, resp, rng=rng)
    subjects = [f"s{k}" for k in range(6)]

    for _ in range(30):
        for subject in subjects:
            mode = rng.choice(["pooled", "directed", "auction"])
            if mode == "pooled":
                agg.request(subject, payload=subject)
            elif mode == "directed":
                agg.request(subject, payload=subject, target=rng.choice(list(resp)))
            else:
                names = rng.sample(list(resp), rng.randint(1, 4))
                agg.auction(subject, {n: {"t": rng.randint(1, 9)} for n in names},
                            lowest_time, payload=subject)
        net.run()
        invariants(agg, resp)
        clk.advance(rng.choice([0, 0.5, 1, 3]))
        agg.tick(); net.run()
        if rng.random() < 0.4:
            held = [s for s, h in agg.handshakes.items() if h.receiver]
            if held:
                subject = rng.choice(held)
                hs = agg.handshakes[subject]
                if rng.random() < 0.5:
                    net.from_instance(resp[hs.receiver].get_completed(subject))
                else:
                    agg.cancel(subject)
                net.run()
    net.run()
    invariants(agg, resp)

    clk.advance(60)
    for _ in range(5):
        agg.tick(); net.run()
    for r in resp.values():
        assert not r.reserved, f"leaked reservation: {list(r.reserved)}"


@pytest.mark.parametrize("seed", range(40))
def test_fuzz_pooled_pairing_never_double_binds(seed):
    rng = random.Random(1000 + seed)
    clk = Clock()
    agg = FakeAggregate(clk)
    resp = {f"i{k}": HandshakeResponder(f"i{k}", clock=clk, max_reserved=1) for k in range(3)}
    net = Net(agg, resp, rng=rng)
    agents = [f"a{k}" for k in range(5)]

    for _ in range(25):
        for agent in agents:
            agg.request(agent, payload=agent)                   # every discovery tick
        net.run()
        invariants(agg, resp)
        clk.advance(rng.choice([0.5, 1, 3]))
        agg.tick(); net.run()
    invariants(agg, resp)
    bound = [r.active for r in resp.values() if r.active]
    assert len(bound) == len(set(bound)) == 3                   # 3 instances, 3 distinct agents


@pytest.mark.parametrize("seed", range(20))
def test_fuzz_with_message_loss_never_crashes_or_leaks(seed):
    rng = random.Random(5000 + seed)
    clk = Clock()
    agg = FakeAggregate(clk)
    resp = {f"i{k}": HandshakeResponder(f"i{k}", clock=clk, timeout=4.0,
                                        bid_fn=lambda h: Bid(h["t"])) for k in range(3)}
    net = Net(agg, resp, rng=rng, drop=0.15)
    for _ in range(30):
        subject = f"s{rng.randint(0, 4)}"
        names = rng.sample(list(resp), rng.randint(1, 3))
        agg.auction(subject, {n: {"t": rng.randint(1, 9)} for n in names},
                    lowest_time, payload=subject)
        net.run()
        invariants(agg, resp)
        clk.advance(rng.choice([0.5, 3]))
        agg.tick(); net.run()

    net.drop = 0.0                                              # the link comes back
    clk.advance(60)
    for _ in range(8):
        agg.tick(); net.run()
        clk.advance(10)
    invariants(agg, resp)