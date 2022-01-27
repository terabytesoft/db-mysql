<?php

declare(strict_types=1);

namespace Yiisoft\Db\Mysql;

use PDO;
use PDOException;
use Psr\Log\LogLevel;
use Yiisoft\Db\Cache\QueryCache;
use Yiisoft\Db\Cache\SchemaCache;
use Yiisoft\Db\Command\Command;
use Yiisoft\Db\Connection\Connection;
use Yiisoft\Db\Connection\ConnectionPDOInterface;
use Yiisoft\Db\Driver\DriverInterface;
use Yiisoft\Db\Driver\PDOInterface;
use Yiisoft\Db\Exception\Exception;
use Yiisoft\Db\Exception\InvalidConfigException;

use function constant;

/**
 * The class Connection represents a connection to a database via [PDO](https://secure.php.net/manual/en/book.pdo.php).
 */
final class ConnectionPDOMysql extends Connection implements ConnectionPDOInterface
{
    private PDOMysqlDriver $driver;
    private QueryCache $queryCache;
    private SchemaCache $schemaCache;

    public function __construct(PDOMysqlDriver $driver, QueryCache $queryCache, SchemaCache $schemaCache)
    {
        $this->driver = $driver;
        $this->queryCache = $queryCache;
        $this->schemaCache = $schemaCache;

        parent::__construct($queryCache);
    }

    /**
     * Reset the connection after cloning.
     */
    public function __clone()
    {
        $this->master = null;
        $this->slave = null;
        $this->transaction = null;

        if (strncmp($this->driver->getDsn(), 'sqlite::memory:', 15) !== 0) {
            /** reset PDO connection, unless its sqlite in-memory, which can only have one connection */
            $this->driver = clone $this->driver;
            $this->driver->pdo(null);
        }
    }

    /**
     * Close the connection before serializing.
     *
     * @return array
     */
    public function __sleep(): array
    {
        $fields = (array) $this;

        unset(
            $fields["\000" . __CLASS__ . "\000" . 'master'],
            $fields["\000" . __CLASS__ . "\000" . 'slave'],
            $fields["\000" . __CLASS__ . "\000" . 'transaction'],
            $fields["\000" . __CLASS__ . "\000" . 'schema']
        );

        return array_keys($fields);
    }

    public function close(): void
    {
        if (!empty($this->master)) {
            $this->driver->PDO(null);
            $this->master->close();
            $this->master = null;
        }

        if ($this->driver->getPDO() !== null) {
            $this->logger?->log(
                LogLevel::DEBUG,
                'Closing DB connection: ' . $this->driver->getDsn() . ' ' . __METHOD__,
            );

            $this->driver->PDO(null);
            $this->transaction = null;
        }

        if (!empty($this->slave)) {
            $this->slave->close();
            $this->slave = null;
        }
    }

    public function createCommand(?string $sql = null, array $params = []): Command
    {
        if ($sql !== null) {
            $sql = $this->quoteSql($sql);
        }

        $command = new Command($this, $this->queryCache, $sql);

        if ($this->logger !== null) {
            $command->setLogger($this->logger);
        }

        if ($this->profiler !== null) {
            $command->setProfiler($this->profiler);
        }

        return $command->bindValues($params);
    }

    public function getDriver(): PDOInterface
    {
        return $this->driver;
    }

    /**
     * Returns the name of the DB driver.
     *
     * @return string name of the DB driver
     */
    public function getDriverName(): string
    {
        return 'mysql';
    }

    /**
     * Returns the PDO instance for the currently active master connection.
     *
     * This method will open the master DB connection and then return {@see pdo}.
     *
     * @throws Exception|InvalidConfigException
     *
     * @return PDO|null the PDO instance for the currently active master connection.
     */
    public function getMasterPdo(): PDO|null
    {
        $this->open();
        return $this->driver->getPDO();
    }

    public function getSchema(): Schema
    {
        return new Schema($this, $this->schemaCache);
    }

    /**
     * Returns the PDO instance for the currently active slave connection.
     *
     * When {@see enableSlaves} is true, one of the slaves will be used for read queries, and its PDO instance will be
     * returned by this method.
     *
     * @param bool $fallbackToMaster whether to return a master PDO in case none of the slave connections is available.
     *
     * @throws Exception|InvalidConfigException
     *
     * @return PDO|null the PDO instance for the currently active slave connection. `null` is returned if no slave
     * connection is available and `$fallbackToMaster` is false.
     */
    public function getSlavePdo(bool $fallbackToMaster = true): ?PDO
    {
        $db = $this->getSlave(false);

        if ($db === null) {
            return $fallbackToMaster ? $this->getMasterPdo() : null;
        }

        return $db->getDriver()->getPdo();
    }

    public function open(): void
    {
        if (!empty($this->driver->getPDO())) {
            return;
        }

        if ($this->masters !== []) {
            $db = $this->getMaster();

            if ($db !== null) {
                return;
            }

            throw new InvalidConfigException('None of the master DB servers is available.');
        }

        if (empty($this->driver->getDsn())) {
            throw new InvalidConfigException('Connection::dsn cannot be empty.');
        }

        $token = 'Opening DB connection: ' . $this->driver->getDsn();

        try {
            $this->logger?->log(LogLevel::INFO, $token);
            $this->profiler?->begin($token, [__METHOD__]);
            $this->driver->createConnectionInstance();
            $this->initConnection();
            $this->profiler?->end($token, [__METHOD__]);
        } catch (PDOException $e) {
            $this->profiler?->end($token, [__METHOD__]);
            $this->logger?->log(LogLevel::ERROR, $token);

            throw new Exception($e->getMessage(), (array) $e->errorInfo, $e);
        }
    }

    /**
     * Returns a value indicating whether the DB connection is established.
     *
     * @return bool whether the DB connection is established
     */
    public function isActive(): bool
    {
        return $this->driver->getPDO() !== null;
    }

    /**
     * Initializes the DB connection.
     *
     * This method is invoked right after the DB connection is established.
     *
     * The default implementation turns on `PDO::ATTR_EMULATE_PREPARES`.
     *
     * if {@see emulatePrepare} is true, and sets the database {@see charset} if it is not empty.
     *
     * It then triggers an {@see EVENT_AFTER_OPEN} event.
     */
    protected function initConnection(): void
    {
        $pdo = $this->driver->getPDO();

        if ($pdo !== null) {
            $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

            if ($this->getEmulatePrepare() !== null && constant('PDO::ATTR_EMULATE_PREPARES')) {
                $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $this->getEmulatePrepare());
            }

            $charset = $this->driver->getCharset();

            if ($charset !== null) {
                $pdo->exec('SET NAMES ' . $pdo->quote($charset));
            }
        }
    }
}
