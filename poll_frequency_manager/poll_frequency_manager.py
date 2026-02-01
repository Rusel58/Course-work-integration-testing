class PollFrequencyManager:
    @staticmethod
    def check_input_parameters(
        transition_duration: int,
        initial_poll_freq: int,
        final_poll_freq: int,
    ):
        for param_name, param_value in {
            "transition_duration": transition_duration,
            "initial_poll_freq": initial_poll_freq,
            "final_poll_freq": final_poll_freq,
        }.items():
            if param_value <= 0:
                raise ValueError(f"{param_name} can't be non-positive")

        if initial_poll_freq < final_poll_freq:
            raise ValueError("Initial poll frequency can't be less than final poll frequency")

    @staticmethod
    def calculate_await_time(
        elapsed_transition_time: float,
        transition_duration: int,
        initial_poll_freq: int,
        final_poll_freq: int,
    ) -> int:
        if elapsed_transition_time >= transition_duration:
            return final_poll_freq

        current_poll_freq = initial_poll_freq - (elapsed_transition_time / transition_duration) * (
            initial_poll_freq - final_poll_freq
        )

        current_poll_freq = int(current_poll_freq)

        current_poll_freq = max(current_poll_freq, final_poll_freq)

        return current_poll_freq

