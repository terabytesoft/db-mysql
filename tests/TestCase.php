<?php

declare(strict_types=1);

namespace Yiisoft\Db\Mysql\Tests;

use PHPUnit\Framework\TestCase as AbstractTestCase;
use Yiisoft\Db\Mysql\Connection;
use Yiisoft\Db\TestUtility\TestTrait;
use Yiisoft\Db\Mysql\PDOMysqlDriver;

class TestCase extends AbstractTestCase
{
    use TestTrait;

    protected const DB_CONNECTION_CLASS = \Yiisoft\Db\Mysql\Connection::class;
    protected const DB_DRIVERNAME = 'mysql';
    protected const DB_DRIVER_CLASS = PDOMysqlDriver::class;
    protected const DB_DSN = 'mysql:host=127.0.0.1;dbname=yiitest;port=3306';
    protected const DB_FIXTURES_PATH = __DIR__ . '/Fixture/mysql.sql';
    protected const DB_USERNAME = 'root';
    protected const DB_PASSWORD = '';
    protected const DB_CHARSET = 'UTF8MB4';
    protected array $dataProvider;
    protected string $likeEscapeCharSql = '';
    protected array $likeParameterReplacements = [];
    protected Connection $connection;

    protected function setUp(): void
    {
        parent::setUp();
        $PDOMysqlDriver = new PDOMysqlDriver(self::DB_DSN, self::DB_USERNAME, self::DB_PASSWORD);
        $this->connection = $this->createConnection($PDOMysqlDriver);
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->connection->close();
        unset(
            $this->cache,
            $this->connection,
            $this->logger,
            $this->queryCache,
            $this->schemaCache,
            $this->profiler
        );
    }
}
