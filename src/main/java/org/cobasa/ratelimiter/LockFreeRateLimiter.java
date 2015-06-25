package org.cobasa.ratelimiter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple lock-free RateLimiter class experiment.
 * 
 * Internally, it uses an AtomicLong variable to store the the
 * "time stamp in milliseconds of the last update" mod 2**48 and the number of
 * used tokens.
 * 
 * Note we use 48 bits to store the time stamp of the last update, and 16 bits
 * (short) for the maximum number of tokens.
 * 
 * 
 */
public class LockFreeRateLimiter {

	static private final long MAX_TIME = 1 << (Long.SIZE - Short.SIZE);
	static private final long PERIOD_MS = 1000;
	private final AtomicLong bucket;
	private final short tokensPerSecond;

	/**
	 * Creates a lock-free RateLimiter object
	 * 
	 * @param tokensPerSecond
	 *            the maximum number of tokens to be allowed per second
	 */
	LockFreeRateLimiter(short tokensPerSecond) {
		bucket = new AtomicLong();
		this.tokensPerSecond = tokensPerSecond;
	}

	/**
	 * Try to acquire the given number of tokens in a lock-free fashion. It will
	 * return the number of acquired tokens, which may vary from 0 to the actual
	 * number of tokens.
	 * 
	 * @param tokens
	 *            requested number of tokens
	 * @return actual number of acquired tokens (from 0 to tokens)
	 */
	public short tryAcquire(short tokens) {
		while (true) {
			long current = bucket.get();

			if ((now() - lastUpdate(current)) < PERIOD_MS) {
				short usedTokens = tokens(current);

				if (usedTokens > tokensPerSecond) {
					// All requested tokens are rate-limited
					return 0;
				}

				short actualTokens = (short) Math.min(tokens, tokensPerSecond - usedTokens);

				if (tryUpdate(current, (short) (usedTokens + actualTokens))) {
					// Accept actualTokens, and rate-limit tokens - actualTokens
					return actualTokens;
				} else {
					continue;
				}

			} else {
				// PERIOD_MS is over, reset token counter
				if (tryUpdate(current, (short) (tokensPerSecond - tokens))) {
					return tokens;
				} else {
					continue;
				}
			}
		}
	}

	/**
	 * Try to acquire one token in a lock-free fashion. It will return the
	 * acquired token or 0.
	 * 
	 * @return 1 if the token is acquired, 0 otherwise.
	 */
	public short tryAcquire() {
		return tryAcquire((short) 1);
	}

	private long lastUpdate(long current) {
		return current >> Short.SIZE;
	}

	private short tokens(long current) {
		return (short) current;
	}

	private long now() {
		return System.currentTimeMillis() % MAX_TIME;
	}

	private boolean tryUpdate(long current, short permits) {
		long newVal = now() << Short.SIZE;
		newVal |= permits;

		return bucket.compareAndSet(current, newVal);
	}
}
