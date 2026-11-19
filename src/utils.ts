/**
 * Generates a random integer between 0 and N (inclusive).
 * @param n - The maximum integer value (must be >= 0).
 * @returns A random integer from [0, n].
 */
export function getRandomInt(n: number): number {
    if (!Number.isInteger(n)) {
        throw new Error('n must be an integer');
    }
    if (n < 0) {
        throw new Error('n must be a non-negative integer');
    }
    // Multiply by (n + 1) to make the range inclusive of n
    return Math.floor(Math.random() * (n + 1));
}

/**
 * Generates a GUID/UUID v4 (random).
 * @returns A string in the format 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.
 */
export function generateGuid(): string {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
        const r = Math.random() * 16 | 0;
        const v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

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
    const unknownErrorMessage = "unknown-error";
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

/**
 * Return true if the conten-type is json
 * @param contentType content-type.
 * @returns true or false
 */
export function is_json_content(contentType: string) {
    const mimeType = contentType.split(';')[0].trim().toLowerCase();
    return (mimeType === 'application/json' || mimeType.endsWith('+json'));
}