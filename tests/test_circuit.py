"""The breaker's state machine, with the clock under the test's control."""

from __future__ import annotations

from berth.circuit import CircuitBreaker, State


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make(**kwargs) -> tuple[CircuitBreaker, Clock]:
    clock = Clock()
    return CircuitBreaker(clock=clock, **kwargs), clock


def test_a_new_breaker_lets_everything_through():
    breaker, _clock = make()
    assert breaker.state is State.CLOSED
    assert all(breaker.allows() for _ in range(100))


def test_it_trips_on_a_run_of_failures():
    breaker, _clock = make(failure_threshold=3)
    for _ in range(2):
        breaker.failed()
    assert breaker.state is State.CLOSED

    breaker.failed()
    assert breaker.state is State.OPEN
    assert breaker.allows() is False
    assert breaker.trips == 1


def test_a_success_clears_the_run():
    """Three failures spread across a working service is not an outage."""
    breaker, _clock = make(failure_threshold=3)
    breaker.failed()
    breaker.failed()
    breaker.succeeded()
    breaker.failed()
    breaker.failed()
    assert breaker.state is State.CLOSED


def test_an_open_breaker_rejects_until_the_cooldown_elapses():
    breaker, clock = make(failure_threshold=1, cooldown_s=5.0)
    breaker.failed()

    clock.advance(4.9)
    assert breaker.allows() is False
    assert breaker.rejected > 0

    clock.advance(0.2)
    assert breaker.allows() is True
    assert breaker.state is State.HALF_OPEN


def test_half_open_only_lets_a_couple_of_probes_through():
    """The point of half-open: risk two requests on an unproven backend, not
    the whole load."""
    breaker, clock = make(failure_threshold=1, cooldown_s=1.0, probe_limit=2)
    breaker.failed()
    clock.advance(1.1)

    assert breaker.allows() is True
    assert breaker.allows() is True
    assert breaker.allows() is False, "a third probe should be refused"


def test_enough_good_probes_close_it():
    breaker, clock = make(failure_threshold=1, cooldown_s=1.0, success_threshold=2)
    breaker.failed()
    clock.advance(1.1)

    breaker.allows(); breaker.succeeded()
    assert breaker.state is State.HALF_OPEN
    breaker.allows(); breaker.succeeded()
    assert breaker.state is State.CLOSED
    assert breaker.allows() is True


def test_one_failed_probe_re_opens_it_immediately():
    """No second chances while half-open. The backend was given its chance."""
    breaker, clock = make(failure_threshold=5, cooldown_s=1.0)
    for _ in range(5):
        breaker.failed()
    clock.advance(1.1)

    breaker.allows()
    breaker.failed()
    assert breaker.state is State.OPEN
    assert breaker.trips == 2


def test_the_cooldown_restarts_on_a_failed_probe():
    breaker, clock = make(failure_threshold=1, cooldown_s=5.0)
    breaker.failed()
    clock.advance(5.1)
    breaker.allows()
    breaker.failed()

    clock.advance(4.9)
    assert breaker.allows() is False
    clock.advance(0.2)
    assert breaker.allows() is True


def test_reset_puts_it_straight_back_to_closed():
    """Used when active health checking proves the backend is answering again,
    so it does not have to sit out the rest of its cooldown."""
    breaker, _clock = make(failure_threshold=1)
    breaker.failed()
    assert breaker.is_open

    breaker.reset()
    assert breaker.state is State.CLOSED
    assert breaker.allows() is True


def test_the_snapshot_reports_what_happened():
    breaker, _clock = make(failure_threshold=2)
    breaker.failed()
    breaker.failed()
    breaker.allows()
    snapshot = breaker.snapshot()
    assert snapshot["state"] == "open"
    assert snapshot["trips"] == 1
    assert snapshot["rejected"] == 1
