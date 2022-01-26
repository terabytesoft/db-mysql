<?php

declare(strict_types=1);

namespace Yiisoft\Db\Mysql;

use PDO;
use Yiisoft\Db\Driver\PDODriverInterface;

use function strncmp;

final class PDOMysqlDriver implements PDODriverInterface
{
    private array $attributes = [];
    private ?string $charset = null;
    private string $dsn = '';
    private string $username = '';
    private string $password = '';
    private ?PDO $pdo = null;

    public function __construct(string $dsn, string $username = '', string $password = '', array $attributes = [])
    {
        $this->dsn = $dsn;
        $this->username = $username;
        $this->password = $password;
        $this->attributes = $attributes;
    }

    public function __sleep(): array
    {
        $fields = (array) $this;
        unset($fields["\000" . __CLASS__ . "\000" . 'pdo']);
        return array_keys($fields);
    }

    public function attributes(array $attributes): void
    {
        $this->attributes = $attributes;
    }

    public function createPdoInstance(): PDO
    {
        return $this->pdo = new PDO($this->dsn, $this->username, $this->password, $this->attributes);
    }

    public function charset(?string $charset): void
    {
        $this->charset = $charset;
    }

    public function getCharset(): ?string
    {
        return $this->charset;
    }

    public function getDsn(): string
    {
        return $this->dsn;
    }

    public function getPassword(): string
    {
        return $this->password;
    }

    public function getPDO(): ?PDO
    {
        return $this->pdo;
    }

    public function getUsername(): string
    {
        return $this->username;
    }

    public function PDO(?PDO $pdo): void
    {
        $this->pdo = $pdo;
    }

    public function password(string $password): void
    {
        $this->password = $password;
    }

    public function username(string $username): void
    {
        $this->username = $username;
    }
}
