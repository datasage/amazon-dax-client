<?php

declare(strict_types=1);

namespace Amazon\Dax\Exception;

use Exception;

/**
 * Base exception class for DAX client errors
 */
class DaxException extends Exception
{
    private ?string $errorCode = null;
    private ?string $requestId = null;

    /**
     * Constructor
     *
     * @param string $message Error message
     * @param int $code Error code
     * @param Exception|null $previous Previous exception
     * @param string|null $errorCode DAX-specific error code
     * @param string|null $requestId Request ID from DAX service
     */
    public function __construct(
        string $message = '',
        int $code = 0,
        ?Exception $previous = null,
        ?string $errorCode = null,
        ?string $requestId = null
    ) {
        parent::__construct($message, $code, $previous);
        $this->errorCode = $errorCode;
        $this->requestId = $requestId;
    }

    /**
     * Get the DAX-specific error code
     *
     * @return string|null
     */
    public function getErrorCode(): ?string
    {
        return $this->errorCode;
    }

    /**
     * Get the request ID
     *
     * @return string|null
     */
    public function getRequestId(): ?string
    {
        return $this->requestId;
    }

    /**
     * Set the error code
     *
     * @param string|null $errorCode
     * @return self
     */
    public function setErrorCode(?string $errorCode): self
    {
        $this->errorCode = $errorCode;
        return $this;
    }

    /**
     * Set the request ID
     *
     * @param string|null $requestId
     * @return self
     */
    public function setRequestId(?string $requestId): self
    {
        $this->requestId = $requestId;
        return $this;
    }
}
