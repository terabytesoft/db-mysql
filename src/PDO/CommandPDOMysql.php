<?php

declare(strict_types=1);

namespace Yiisoft\Db\Mysql\PDO;

use PDOException;
use Yiisoft\Db\Cache\QueryCache;
use Yiisoft\Db\Command\Command;
use Yiisoft\Db\Connection\ConnectionPDOInterface;
use Yiisoft\Db\Exception\Exception;
use Yiisoft\Db\Exception\InvalidConfigException;
use Yiisoft\Db\Exception\NotSupportedException;
use Yiisoft\Db\Mysql\DDLCommand;
use Yiisoft\Db\Mysql\DMLCommand;
use Yiisoft\Db\Query\QueryBuilderInterface;
use Yiisoft\Db\Schema\QuoterInterface;
use Yiisoft\Db\Schema\SchemaInterface;

final class CommandPDOMysql extends Command
{
    public function __construct(
        private ConnectionPDOInterface $db,
        QueryBuilderInterface $queryBuilder,
        QueryCache $queryCache,
        QuoterInterface $quoter,
        private SchemaInterface $schema
    ) {
        parent::__construct($queryBuilder, $queryCache, $quoter, $schema);
    }

    /**
     * @param string $name
     * @param string $table
     * @param string $expression
     *
     * @throws NotSupportedException Method not supported by Mysql.
     *
     * @return static the command object itself.
     */
    public function addCheck(string $name, string $table, string $expression): self
    {
        throw new NotSupportedException(CommandPDOMysql::class . '::addCheck is not supported by MySQL.');
    }

    public function getDDLCommand(): DDLCommand
    {
        return new DDLCommand($this, $this->quoter, $this->schema);
    }

    public function getDMLCommand(): DMLCommand
    {
        return new DMLCommand($this, $this->quoter, $this->schema);
    }

    /**
     * @throws \Exception|Exception|PDOException|InvalidConfigException
     */
    public function prepare(?bool $forRead = null): void
    {
        if (isset($this->pdoStatement)) {
            $this->bindPendingParams();

            return;
        }

        $sql = $this->getSql() ?? '';

        if ($this->db->getTransaction()) {
            /** master is in a transaction. use the same connection. */
            $forRead = false;
        }

        if ($forRead || ($forRead === null && $this->schema->isReadQuery($sql))) {
            $pdo = $this->db->getSlavePdo();
        } else {
            $pdo = $this->db->getMasterPdo();
        }

        try {
            $this->pdoStatement = $pdo?->prepare($sql);
            $this->bindPendingParams();
        } catch (PDOException $e) {
            $message = $e->getMessage() . "\nFailed to prepare SQL: $sql";
            /** @var array|null */
            $errorInfo = $e->errorInfo ?? null;

            throw new Exception($message, $errorInfo, $e);
        }
    }

    protected function getCacheKey(string $method, ?int $fetchMode, string $rawSql): array
    {
        return [
            __CLASS__,
            $method,
            $fetchMode,
            $this->db->getDriver()->getDsn(),
            $this->db->getDriver()->getUsername(),
            $rawSql,
        ];
    }

    protected function internalExecute(?string $rawSql): void
    {
        $attempt = 0;

        while (true) {
            try {
                if (
                    ++$attempt === 1
                    && $this->isolationLevel !== null
                    && $this->db->getTransaction() === null
                ) {
                    $this->db->transaction(fn (?string $rawSql) => $this->internalExecute($rawSql), $this->isolationLevel);
                } else {
                    $this->pdoStatement?->execute();
                }
                break;
            } catch (\Exception $e) {
                $rawSql = $rawSql ?: $this->getRawSql();
                $e = $this->schema->convertException($e, $rawSql);

                if ($this->retryHandler === null || !($this->retryHandler)($e, $attempt)) {
                    throw $e;
                }
            }
        }
    }
}
