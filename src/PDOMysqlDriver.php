<?php

declare(strict_types=1);

namespace Yiisoft\Db\Mysql;

use PDO;
use Yiisoft\Db\Driver\PDOInterface;

final class PDOMysqlDriver implements PDOInterface
{
    private array $attributes;
    private ?string $charset = null;
    private string $dsn;
    private string $username;
    private string $password;
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

    /**
     * PDO attributes (name => value) that should be set when calling {@see open()} to establish a DB connection.
     * Please refer to the [PHP manual](http://php.net/manual/en/pdo.setattribute.php) for details about available
     * attributes.
     *
     * @param array $attributes the attributes (name => value) to be set on the DB connection.
     */
    public function attributes(array $attributes): void
    {
        $this->attributes = $attributes;
    }

    /**
     * @return static
     */
    public function createConnectionInstance(): self
    {
        $this->pdo = new PDO($this->dsn, $this->username, $this->password, $this->attributes);
        return $this;
    }

    /**
     * The charset used for database connection. The property is only used for MySQL, PostgreSQL databases. Defaults to
     * null, meaning using default charset as configured by the database.
     *
     * For Oracle Database, the charset must be specified in the {@see dsn}, for example for UTF-8 by appending
     * `;charset=UTF-8` to the DSN string.
     *
     * The same applies for if you're using GBK or BIG5 charset with MySQL, then it's highly recommended to specify
     * charset via {@see dsn} like `'mysql:dbname=mydatabase;host=127.0.0.1;charset=GBK;'`.
     *
     * @param string|null $charset
     */
    public function charset(?string $charset): void
    {
        $this->charset = $charset;
    }

    /**
     * Returns the charset currently used for database connection. The returned charset is only applicable for MySQL,
     * PostgreSQL databases.
     *
     * @return string|null the charset of the pdo instance. Null is returned if the charset is not set yet or not
     * supported by the pdo driver
     */
    public function getCharset(): ?string
    {
        return $this->charset;
    }

    /**
     * Return dsn string for current driver.
     */
    public function getDsn(): string
    {
        return $this->dsn;
    }

    /**
     * Returns the password for establishing DB connection.
     *
     * @return string the password for establishing DB connection.
     */
    public function getPassword(): string
    {
        return $this->password;
    }

    /**
     * Returns the PDO instance.
     *
     * @return PDO|null the PDO instance, null if the connection is not established yet.
     */
    public function getPDO(): ?PDO
    {
        return $this->pdo;
    }

    /**
     * Returns the username for establishing DB connection.
     *
     * @return string the username for establishing DB connection.
     */
    public function getUsername(): string
    {
        return $this->username;
    }

    /**
     * Set the PDO connection.
     */
    public function PDO(?PDO $pdo): void
    {
        $this->pdo = $pdo;
    }

    /**
     * The password for establishing DB connection. Defaults to `null` meaning no password to use.
     *
     * @param string $password the password for establishing DB connection.
     */
    public function password(string $password): void
    {
        $this->password = $password;
    }


    /**
     * The username for establishing DB connection. Defaults to `null` meaning no username to use.
     *
     * @param string $username the username for establishing DB connection.
     */
    public function username(string $username): void
    {
        $this->username = $username;
    }
}
