/**
 * generates a secure random token of the specified length using the Web Crypto API.
 * @param length The length of the token to generate.
 * @returns A secure random token string.
 */
export function generateSecureToken(length: number = 10): string {
    const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let result = "";
    // Generate cryptographically strong random values
    const randomValues = new Uint32Array(length);
    crypto.getRandomValues(randomValues);
    for (let i = 0; i < length; i++) {
        result += charset[randomValues[i] % charset.length];
    }
    return result;
}

/**
 * Pauses execution for a specified number of milliseconds.
 * @param ms The number of milliseconds to sleep.
 * @returns A promise that resolves after the specified delay.
 */
export function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Extracts a human-readable error message from various types of error objects.
 * @param error The error object or value to extract the message from.
 * @param defaultMessage An optional default message to use if no message can be extracted.
 * @returns A human-readable error message.
 */
export function error_message(error: any, defaultMessage?: string): string {
    const unknownErrorMessage = "(Unknown error)";
    if (!error) {
        return defaultMessage || unknownErrorMessage;
    } else if (error instanceof Error) {
        return error.message;
    } else if (typeof error.message === 'string' && error.message) {
        return error.message;
    } else if (typeof error === 'string') {
        return error;
    } else {
        return defaultMessage || JSON.stringify(error) || unknownErrorMessage;
    }
}