# Wireless for Weld

What the radio does to a fleet and what Weld does about it. Weld never tunes the radio.
Weld tunes topic choice, message size, publish rate, reliability setting, and producer slowdown.
Air is shared: capacity is set by interference, not raw bandwidth.
Same distance does not mean same link; shadowing moves the link while robots stand still.
Partition is expected state, not error state; the graph index is partition local and merge is deterministic.

## Rules

- Loss has memory. Errors arrive in bursts during fades and contention tails, not coin flips. Chaos tests use a two-state link: good near 0% for seconds, bad at 30-100% for a dwell such as 300 ms.
- History depth must cover the tail: `D >= R * T_out + repair slack`. At 100 Hz with 200 ms outage, depth needs at least 20 before slack. Record R, T_out, and D together.
- Coverage at distance d against a threshold: `P_ok(d) = Q((threshold - Pr_mean(d)) / sigma)`. Design for a spot probability such as 75%, never the mean alone.
- Deadline policy: if period P is much shorter than bad-state dwell T_bad, one spell causes many misses. Alert once per outage, then clear once. Program T_bad / P at 0.2, 1, 5, and 20 in fake-clock tests.
- Repair accounting: one run of N missing samples increments the lost counter by N as one gap event. Cap repair bytes, coalesce ranges, jitter responses.
- Small vs bulk: small control topics at higher rate stay fresh; bulk goes best-effort and sheds first. Bound message sizes; one symbol error kills a large frame and its retry.
- Discovery on WiFi: use explicit unicast peers, not multicast. Keep membership small, periodic, and partition scoped. No modulation or channel knob exists in the Weld API.

## Timers

- Liveliness heartbeat period 0.25 s, check timers 0.2 s, manager bond timeout 4.0 s, service timeout 5.0 s.
- Liveliness lease spans several fades; one missed heartbeat on a noisy channel is not death.
- Sub-second admission and deactivation bounds; action goal default 60 s when the client sets none.
- Backward clock jumps rebase deadline tracking and surface a clock-mismatch error, never freeze silently.

## Chaos matrix

- Signal: sweep SNR near threshold, where 1-2 dB swings loss by 10x; anchor at -101 dBm noise floor (20 MHz) and -95 dBm sensitivity.
- Fade rate: two-state link with programmed dwell; T_bad / P ratios at 0.2, 1, 5, and 20.
- Load: offered load A = arrival rate times holding time; bulk airtime capped near one third of measured goodput, control near 10 Hz on a separate profile.
- Partition: split, query and publish each side, rejoin; each side reports only its segment, rejoin converges to the same sorted order with no duplicates.

## Cheat sheet

- dB: `gain_dB = 10*log10(P_out/P_in)`; 3 dB doubles, 10 dB times ten. Never mix dB with dBm.
- Load: `A = lambda * H`; sizing needs channels C > A or the queue never drains.
- Loss tail: `P(T > s) = e^(-s/H)`; at 5 arrivals/s, P(zero in 0.2 s) = 0.368.
- Depth: `D >= R * T_out + slack`; a depth without its outage target is unreviewable.
- Ceiling: `C = B*log2(1+SNR)`; budget measured goodput with margin, never the ceiling.
- QoS facade: advertise reliable plus transient-local, implement bounded retention with drop-oldest and lost counters (`docs/wire.md`).
