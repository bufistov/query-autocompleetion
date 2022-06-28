package org.bufistov.autocomplete;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;

import java.util.Random;

@RequiredArgsConstructor
public class UniformRandomInterval implements RandomInterval {
    private final Random random;

    @Value("${org.bufistov.autocomplete.max_retry_delay_millis}")
    private int maxDelayMillis;

    @Override
    public long getMillis() {
        return random.nextInt(maxDelayMillis);
    }
}
