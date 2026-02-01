import pytest
from poll_frequency_manager.poll_frequency_manager import PollFrequencyManager


def test_valid_parameters():
    PollFrequencyManager.check_input_parameters(
        transition_duration=100,
        initial_poll_freq=10,
        final_poll_freq=5,
    )


def test_non_positive_transition_duration():
    with pytest.raises(ValueError) as exc_info:
        PollFrequencyManager.check_input_parameters(
            transition_duration=0,
            initial_poll_freq=10,
            final_poll_freq=5,
        )
    assert "transition_duration can't be non-positive" in str(exc_info.value)


def test_non_positive_initial_poll_freq():
    with pytest.raises(ValueError) as exc_info:
        PollFrequencyManager.check_input_parameters(
            transition_duration=100,
            initial_poll_freq=0,
            final_poll_freq=5,
        )
    assert "initial_poll_freq can't be non-positive" in str(exc_info.value)


def test_non_positive_final_poll_freq():
    with pytest.raises(ValueError) as exc_info:
        PollFrequencyManager.check_input_parameters(
            transition_duration=100,
            initial_poll_freq=10,
            final_poll_freq=0,
        )
    assert "final_poll_freq can't be non-positive" in str(exc_info.value)


def test_initial_poll_freq_less_than_final():
    with pytest.raises(ValueError) as exc_info:
        PollFrequencyManager.check_input_parameters(
            transition_duration=100,
            initial_poll_freq=5,
            final_poll_freq=10,
        )
    assert "Initial poll frequency can't be less than final poll frequency" == str(exc_info.value)


def test_elapsed_time_greater_than_transition_duration():
    result = PollFrequencyManager.calculate_await_time(
        elapsed_transition_time=100,
        transition_duration=50,
        initial_poll_freq=10,
        final_poll_freq=5,
    )
    assert result == 5


def test_elapsed_time_equal_to_transition_duration():
    result = PollFrequencyManager.calculate_await_time(
        elapsed_transition_time=50,
        transition_duration=50,
        initial_poll_freq=10,
        final_poll_freq=5,
    )
    assert result == 5


def test_elapsed_time_zero():
    result = PollFrequencyManager.calculate_await_time(
        elapsed_transition_time=0,
        transition_duration=50,
        initial_poll_freq=10,
        final_poll_freq=5,
    )
    assert result == 10


def test_middle_of_transition():
    result = PollFrequencyManager.calculate_await_time(
        elapsed_transition_time=25,
        transition_duration=50,
        initial_poll_freq=10,
        final_poll_freq=5,
    )
    assert result == 7


def test_near_end_of_transition():
    result = PollFrequencyManager.calculate_await_time(
        elapsed_transition_time=45,
        transition_duration=50,
        initial_poll_freq=10,
        final_poll_freq=5,
    )
    assert result == 5


def test_initial_equals_final():
    result = PollFrequencyManager.calculate_await_time(
        elapsed_transition_time=25,
        transition_duration=50,
        initial_poll_freq=5,
        final_poll_freq=5,
    )
    assert result == 5


def test_fractional_result_rounds_down():
    result = PollFrequencyManager.calculate_await_time(
        elapsed_transition_time=30,
        transition_duration=100,
        initial_poll_freq=10,
        final_poll_freq=3,
    )
    assert result == 7


def test_result_clamped_to_final_frequency():
    result = PollFrequencyManager.calculate_await_time(
        elapsed_transition_time=99,
        transition_duration=100,
        initial_poll_freq=100,
        final_poll_freq=10,
    )
    assert result == 10


def test_result_transition_no_use():
    result = PollFrequencyManager.calculate_await_time(
        elapsed_transition_time=400,
        transition_duration=600,
        initial_poll_freq=300,
        final_poll_freq=300,
    )
    assert result == 300


def test_result_transition_no_use2():
    result = PollFrequencyManager.calculate_await_time(
        elapsed_transition_time=600,
        transition_duration=400,
        initial_poll_freq=300,
        final_poll_freq=300,
    )
    assert result == 300

