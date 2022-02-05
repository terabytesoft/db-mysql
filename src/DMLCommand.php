<?php

declare(strict_types=1);

namespace Yiisoft\Db\Mysql;

use InvalidArgumentException;
use Throwable;
use Yiisoft\Db\Command\CommandInterface;
use Yiisoft\Db\Command\DMLCommand as AbstractDMLCommand;
use Yiisoft\Db\Exception\Exception;
use Yiisoft\Db\Exception\InvalidConfigException;
use Yiisoft\Db\Schema\QuoterInterface;
use Yiisoft\Db\Schema\SchemaInterface;

final class DMLCommand extends AbstractDMLCommand
{
    public function __construct(
        private CommandInterface $command,
        private QuoterInterface $quoter,
        private SchemaInterface $schema
    ){
        parent::__construct($quoter);
    }

    /**
     * Creates a SQL statement for resetting the sequence value of a table's primary key.
     *
     * The sequence will be reset such that the primary key of the next new row inserted will have the specified value
     * or 1.
     *
     * @param string $tableName the name of the table whose primary key sequence will be reset.
     * @param array|int|string|null $value the value for the primary key of the next new row inserted. If this is not
     * set, the next new row's primary key will have a value 1.
     *
     * @throws Exception|InvalidArgumentException|InvalidConfigException|Throwable
     *
     * @return string the SQL statement for resetting sequence.
     */
    public function resetSequence(string $tableName, array|int|string|null $value = null): string
    {
        $table = $this->schema->getTableSchema($tableName);

        if ($table !== null && $table->getSequenceName() !== null) {
            $tableName = $this->quoter->quoteTableName($tableName);

            if ($value === null) {
                $pk = $table->getPrimaryKey();
                $key = (string) reset($pk);
                $value = (int) $this->command->setSql("SELECT MAX(`$key`) FROM $tableName")->queryScalar() + 1;
            } else {
                $value = (int) $value;
            }

            return "ALTER TABLE $tableName AUTO_INCREMENT=$value";
        }

        if ($table === null) {
            throw new InvalidArgumentException("Table not found: $tableName");
        }

        throw new InvalidArgumentException("There is no sequence associated with table '$tableName'.");
    }
}
