from crawler.scheduler_retry_policy import SchedulerRetryPolicy


def test_retry_transition_uses_exponential_backoff_and_retry_intent():
    policy = SchedulerRetryPolicy(
        retry_backoff_seconds=5.0,
        max_retry_backoff_seconds=12.0,
        retry_intent="retry",
    )

    transition = policy.failure_transition(
        fail_streak=1,
        discovery_value=1.25,
        retryable=True,
        error="timeout",
        backoff_seconds=None,
        now=100.0,
    )

    assert transition.retryable is True
    assert transition.next_fail_streak == 2
    assert transition.next_scheduler_score == 0.45
    assert transition.next_fetch_at == 110.0
    assert transition.current_intent == "retry"
    assert transition.last_error == "timeout"
    assert transition.terminal_reason is None
    assert transition.terminalized_at is None


def test_terminal_transition_marks_failure_without_retry_intent():
    policy = SchedulerRetryPolicy(
        retry_backoff_seconds=5.0,
        max_retry_backoff_seconds=12.0,
        retry_intent="retry",
    )

    transition = policy.failure_transition(
        fail_streak=0,
        discovery_value=1.25,
        retryable=False,
        error=None,
        backoff_seconds=None,
        now=100.0,
    )

    assert transition.retryable is False
    assert transition.next_fail_streak == 1
    assert transition.next_scheduler_score == 0.75
    assert transition.next_fetch_at == 100.0
    assert transition.current_intent is None
    assert transition.last_error is None
    assert transition.terminal_reason == "failed"
    assert transition.terminalized_at == 100.0
