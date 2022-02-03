<?php

declare(strict_types=1);

namespace Yiisoft\Db\Mysql\PDO;

use JsonException;
use PDO;
use Throwable;
use Yiisoft\Db\Connection\ConnectionInterface;
use Yiisoft\Db\Exception\Exception;
use Yiisoft\Db\Exception\InvalidArgumentException;
use Yiisoft\Db\Exception\InvalidConfigException;
use Yiisoft\Db\Exception\NotSupportedException;
use Yiisoft\Db\Expression\Expression;
use Yiisoft\Db\Expression\ExpressionBuilder;
use Yiisoft\Db\Expression\ExpressionInterface;
use Yiisoft\Db\Expression\JsonExpression;
use Yiisoft\Db\Mysql\JsonExpressionBuilder;
use Yiisoft\Db\Query\Query;
use Yiisoft\Db\Query\QueryBuilder;

use function array_merge;
use function array_values;
use function ctype_digit;
use function implode;
use function preg_match;
use function preg_match_all;
use function preg_replace;
use function reset;
use function trim;
use function version_compare;

/**
 * The class QueryBuilder is the query builder for Mysql databases.
 */
final class QueryBuilderPDOMysql extends QueryBuilder
{
    /**
     * @var array<string, string> mapping from abstract column types (keys) to physical column types (values).
     */
    protected array $typeMap = [
        SchemaPDOMysql::TYPE_PK => 'int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY',
        SchemaPDOMysql::TYPE_UPK => 'int(10) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY',
        SchemaPDOMysql::TYPE_BIGPK => 'bigint(20) NOT NULL AUTO_INCREMENT PRIMARY KEY',
        SchemaPDOMysql::TYPE_UBIGPK => 'bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY',
        SchemaPDOMysql::TYPE_CHAR => 'char(1)',
        SchemaPDOMysql::TYPE_STRING => 'varchar(255)',
        SchemaPDOMysql::TYPE_TEXT => 'text',
        SchemaPDOMysql::TYPE_TINYINT => 'tinyint(3)',
        SchemaPDOMysql::TYPE_SMALLINT => 'smallint(6)',
        SchemaPDOMysql::TYPE_INTEGER => 'int(11)',
        SchemaPDOMysql::TYPE_BIGINT => 'bigint(20)',
        SchemaPDOMysql::TYPE_FLOAT => 'float',
        SchemaPDOMysql::TYPE_DOUBLE => 'double',
        SchemaPDOMysql::TYPE_DECIMAL => 'decimal(10,0)',
        SchemaPDOMysql::TYPE_DATE => 'date',
        SchemaPDOMysql::TYPE_BINARY => 'blob',
        SchemaPDOMysql::TYPE_BOOLEAN => 'tinyint(1)',
        SchemaPDOMysql::TYPE_MONEY => 'decimal(19,4)',
        SchemaPDOMysql::TYPE_JSON => 'json',
    ];

    public function __construct(private ConnectionInterface $db)
    {
        parent::__construct($db->getQuoter(), $db->getSchema());
    }

    /**
     * Builds a SQL command for adding comment to column.
     *
     * @param string $table the table whose column is to be commented. The table name will be properly quoted by the
     * method.
     * @param string $column the name of the column to be commented. The column name will be properly quoted by the
     * method.
     * @param string $comment the text of the comment to be added. The comment will be properly quoted by the method.
     *
     * @throws Exception|Throwable
     *
     * @return string the SQL statement for adding comment on column.
     */
    public function addCommentOnColumn(string $table, string $column, string $comment): string
    {
        /* Strip existing comment which may include escaped quotes */
        $definition = trim(
            preg_replace(
                "/COMMENT '(?:''|[^'])*'/i",
                '',
                $this->getColumnDefinition($table, $column)
            )
        );

        $checkRegex = '/CHECK *(\(([^()]|(?-2))*\))/';
        $check = preg_match($checkRegex, $definition, $checkMatches);

        if ($check === 1) {
            $definition = preg_replace($checkRegex, '', $definition);
        }

        $alterSql = 'ALTER TABLE ' . $this->db->getQuoter()->quoteTableName($table)
            . ' CHANGE ' . $this->db->getQuoter()->quoteColumnName($column)
            . ' ' . $this->db->getQuoter()->quoteColumnName($column)
            . (empty($definition) ? '' : ' ' . $definition)
            . ' COMMENT ' . $this->db->getQuoter()->quoteValue($comment);

        if ($check === 1) {
            $alterSql .= ' ' . $checkMatches[0];
        }

        return $alterSql;
    }

    /**
     * Builds a SQL command for adding comment to table.
     *
     * @param string $table the table whose column is to be commented. The table name will be properly quoted by the
     * method.
     * @param string $comment the text of the comment to be added. The comment will be properly quoted by the method.
     *
     * @throws Exception
     *
     * @return string the SQL statement for adding comment on table.
     */
    public function addCommentOnTable(string $table, string $comment): string
    {
        return 'ALTER TABLE ' . $this->db->getQuoter()->quoteTableName($table)
            . ' COMMENT ' . $this->db->getQuoter()->quoteValue($comment);
    }

    /**
     * @param int|Expression|null $limit
     * @param int|Expression|null $offset
     *
     * @return string the LIMIT and OFFSET clauses.
     */
    public function buildLimit(Expression|int|null $limit, Expression|int|null $offset): string
    {
        $sql = '';

        if ($this->hasLimit($limit)) {
            $sql = 'LIMIT ' . (string) $limit;

            if ($this->hasOffset($offset)) {
                $sql .= ' OFFSET ' . (string) $offset;
            }
        } elseif ($this->hasOffset($offset)) {
            /**
             * limit is not optional in MySQL.
             *
             * http://stackoverflow.com/a/271650/1106908
             * http://dev.mysql.com/doc/refman/5.0/en/select.html#idm47619502796240
             */
            $sql = "LIMIT $offset, 18446744073709551615"; // 2^64-1
        }

        return $sql;
    }

    /**
     * Builds a SQL statement for creating a new index.
     *
     * @param string $name the name of the index. The name will be properly quoted by the method.
     * @param string $table the table that the new index will be created for. The table name will be properly quoted by
     * the method.
     * @param array|string $columns the column(s) that should be included in the index. If there are multiple columns,
     * separate them with commas or use an array to represent them. Each column name will be properly quoted by the
     * method, unless a parenthesis is found in the name.
     * @param bool $unique whether to add UNIQUE constraint on the created index.
     *
     * @psalm-param array<array-key, ExpressionInterface|string>|string $columns
     *
     * @return string the SQL statement for creating a new index.
     *
     * @throws Exception|InvalidArgumentException
     *
     * {@see https://bugs.mysql.com/bug.php?id=48875}
     */
    public function createIndex(string $name, string $table, array|string $columns, bool $unique = false): string
    {
        return 'ALTER TABLE '
            . $this->db->getQuoter()->quoteTableName($table)
            . ($unique ? ' ADD UNIQUE INDEX ' : ' ADD INDEX ')
            . $this->db->getQuoter()->quoteTableName($name)
            . ' (' . $this->buildColumns($columns) . ')';
    }

    /**
     * Builds a SQL statement for enabling or disabling integrity check.
     *
     * @param bool $check  whether to turn on or off the integrity check.
     * @param string $schema the schema of the tables. Meaningless for MySQL.
     * @param string $table  the table name. Meaningless for MySQL.
     *
     * @return string the SQL statement for checking integrity.
     */
    public function checkIntegrity(string $schema = '', string $table = '', bool $check = true): string
    {
        return 'SET FOREIGN_KEY_CHECKS = ' . ($check ? 1 : 0);
    }

    /**
     * @param string $name
     * @param string $table
     *
     * @throws NotSupportedException Method not supported by Mysql.
     *
     * @return string the SQL statement for adding a check constraint to an existing table.
     */
    public function dropCheck(string $name, string $table): string
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by MySQL.');
    }

    /**
     * Builds a SQL command for adding comment to column.
     *
     * @param string $table the table whose column is to be commented. The table name will be properly quoted by the
     * method.
     * @param string $column the name of the column to be commented. The column name will be properly quoted by the
     * method.
     *
     * @throws Exception|Throwable
     *
     * @return string the SQL statement for adding comment on column.
     */
    public function dropCommentFromColumn(string $table, string $column): string
    {
        return $this->addCommentOnColumn($table, $column, '');
    }

    /**
     * Builds a SQL command for adding comment to table.
     *
     * @param string $table the table whose column is to be commented. The table name will be properly quoted by the
     * method.
     *
     * @throws Exception
     *
     * @return string the SQL statement for adding comment on column.
     */
    public function dropCommentFromTable(string $table): string
    {
        return $this->addCommentOnTable($table, '');
    }

    /**
     * Builds a SQL statement for dropping a foreign key constraint.
     *
     * @param string $name the name of the foreign key constraint to be dropped. The name will be properly quoted by the
     * method.
     * @param string $table the table whose foreign is to be dropped. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for dropping a foreign key constraint.
     */
    public function dropForeignKey(string $name, string $table): string
    {
        return 'ALTER TABLE '
            . $this->db->getQuoter()->quoteTableName($table)
            . ' DROP FOREIGN KEY ' . $this->db->getQuoter()->quoteColumnName($name);
    }

    /**
     * Builds a SQL statement for removing a primary key constraint to an existing table.
     *
     * @param string $name the name of the primary key constraint to be removed.
     * @param string $table the table that the primary key constraint will be removed from.
     *
     * @return string the SQL statement for removing a primary key constraint from an existing table.
     */
    public function dropPrimaryKey(string $name, string $table): string
    {
        return 'ALTER TABLE ' . $this->db->getQuoter()->quoteTableName($table) . ' DROP PRIMARY KEY';
    }

    /**
     * Creates a SQL command for dropping a unique constraint.
     *
     * @param string $name the name of the unique constraint to be dropped. The name will be properly quoted by the
     * method.
     * @param string $table the table whose unique constraint is to be dropped. The name will be properly quoted by the
     * method.
     *
     * @return string the SQL statement for dropping an unique constraint.
     */
    public function dropUnique(string $name, string $table): string
    {
        return $this->dropIndex($name, $table);
    }

    /**
     * Converts an abstract column type into a physical column type.
     *
     * The conversion is done using the type map specified in {@see typeMap}.
     * The following abstract column types are supported (using MySQL as an example to explain the corresponding
     * physical types):
     *
     * - `pk`: an auto-incremental primary key type, will be converted into "int(11) NOT NULL AUTO_INCREMENT PRIMARY
     *    KEY"
     * - `bigpk`: an auto-incremental primary key type, will be converted into "bigint(20) NOT NULL AUTO_INCREMENT
     *    PRIMARY KEY"
     * - `upk`: an unsigned auto-incremental primary key type, will be converted into "int(10) UNSIGNED NOT NULL
     *    AUTO_INCREMENT PRIMARY KEY"
     * - `char`: char type, will be converted into "char(1)"
     * - `string`: string type, will be converted into "varchar(255)"
     * - `text`: a long string type, will be converted into "text"
     * - `smallint`: a small integer type, will be converted into "smallint(6)"
     * - `integer`: integer type, will be converted into "int(11)"
     * - `bigint`: a big integer type, will be converted into "bigint(20)"
     * - `boolean`: boolean type, will be converted into "tinyint(1)"
     * - `float``: float number type, will be converted into "float"
     * - `decimal`: decimal number type, will be converted into "decimal"
     * - `datetime`: datetime type, will be converted into "datetime"
     * - `timestamp`: timestamp type, will be converted into "timestamp"
     * - `time`: time type, will be converted into "time"
     * - `date`: date type, will be converted into "date"
     * - `money`: money type, will be converted into "decimal(19,4)"
     * - `binary`: binary data type, will be converted into "blob"
     *
     * If the abstract type contains two or more parts separated by spaces (e.g. "string NOT NULL"), then only the first
     * part will be converted, and the rest of the parts will be appended to the converted result.
     *
     * For example, 'string NOT NULL' is converted to 'varchar(255) NOT NULL'.
     *
     * For some of the abstract types you can also specify a length or precision constraint by appending it in round
     * brackets directly to the type.
     *
     * For example `string(32)` will be converted into "varchar(32)" on a MySQL database. If the underlying DBMS does
     * not support these kind of constraints for a type it will be ignored.
     *
     * If a type cannot be found in {@see typeMap}, it will be returned without any change.
     *
     * @param ColumnSchemaBuilder|string $type abstract column type
     *
     * @throws Exception
     *
     * @return string physical column type.
     */
    public function getColumnType($type): string
    {
        $this->typeMap = array_merge($this->typeMap, $this->defaultTimeTypeMap());
        return parent::getColumnType($type);
    }

    /**
     * Builds a SQL statement for renaming a column.
     *
     * @param string $table the table whose column is to be renamed. The name will be properly quoted by the method.
     * @param string $oldName the old name of the column. The name will be properly quoted by the method.
     * @param string $newName the new name of the column. The name will be properly quoted by the method.
     *
     * @throws Exception|InvalidConfigException|Throwable
     *
     * @return string the SQL statement for renaming a DB column.
     */
    public function renameColumn(string $table, string $oldName, string $newName): string
    {
        $quotedTable = $this->db->getQuoter()->quoteTableName($table);

        /** @psalm-var array<array-key, string> $row */
        $row = $this->db->createCommand('SHOW CREATE TABLE ' . $quotedTable)->queryOne();

        if (isset($row['Create Table'])) {
            $sql = $row['Create Table'];
        } else {
            $row = array_values($row);
            $sql = $row[1];
        }

        if (preg_match_all('/^\s*`(.*?)`\s+(.*?),?$/m', $sql, $matches)) {
            foreach ($matches[1] as $i => $c) {
                if ($c === $oldName) {
                    return "ALTER TABLE $quotedTable CHANGE "
                        . $this->db->getQuoter()->quoteColumnName($oldName) . ' '
                        . $this->db->getQuoter()->quoteColumnName($newName) . ' '
                        . $matches[2][$i];
                }
            }
        }

        /* try to give back a SQL anyway */
        return "ALTER TABLE $quotedTable CHANGE "
            . $this->db->getQuoter()->quoteColumnName($oldName) . ' '
            . $this->db->getQuoter()->quoteColumnName($newName);
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
        $table = $this->db->getTableSchema($tableName);

        if ($table !== null && $table->getSequenceName() !== null) {
            $tableName = $this->db->getQuoter()->quoteTableName($tableName);

            if ($value === null) {
                $pk = $table->getPrimaryKey();
                $key = (string) reset($pk);
                $value = (int) $this->db->createCommand("SELECT MAX(`$key`) FROM $tableName")->queryScalar() + 1;
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

    /**
     * Creates an SQL statement to insert rows into a database table if they do not already exist (matching unique
     * constraints), or update them if they do.
     *
     * For example,
     *
     * ```php
     * $sql = $queryBuilder->upsert('pages', [
     *     'name' => 'Front page',
     *     'url' => 'http://example.com/', // url is unique
     *     'visits' => 0,
     * ], [
     *     'visits' => new Expression('visits + 1'),
     * ], $params);
     * ```
     *
     * The method will properly escape the table and column names.
     *
     * @param string $table the table that new rows will be inserted into/updated in.
     * @param array|Query $insertColumns the column data (name => value) to be inserted into the table or instance of
     * {@see Query} to perform `INSERT INTO ... SELECT` SQL statement.
     * @param bool|array $updateColumns the column data (name => value) to be updated if they already exist. If `true`
     * is passed, the column data will be updated to match the insert column data. If `false` is passed, no update will
     * be performed if the column data already exists.
     * @param array $params the binding parameters that will be generated by this method. They should be bound to the DB
     * command later.
     *
     * @throws Exception|InvalidConfigException|JsonException|NotSupportedException if this is not supported by the
     * underlying DBMS.
     *
     * @return string the resulting SQL.
     */
    public function upsert(string $table, Query|array $insertColumns, bool|array $updateColumns, array &$params): string
    {
        $insertSql = $this->insert($table, $insertColumns, $params);

        /** @var array $uniqueNames */
        [$uniqueNames, , $updateNames] = $this->prepareUpsertColumns($table, $insertColumns, $updateColumns);

        if (empty($uniqueNames)) {
            return $insertSql;
        }

        if ($updateColumns === true) {
            $updateColumns = [];
            /** @var string $name */
            foreach ($updateNames as $name) {
                $updateColumns[$name] = new Expression(
                    'VALUES(' . $this->db->getQuoter()->quoteColumnName($name) . ')'
                );
            }
        } elseif ($updateColumns === false) {
            $columnName = (string) reset($uniqueNames);
            $name = $this->db->getQuoter()->quoteColumnName($columnName);
            $updateColumns = [$name => new Expression($this->db->getQuoter()->quoteTableName($table) . '.' . $name)];
        }

        /**
         *  @psalm-var array<array-key, string> $updates
         *  @psalm-var array<string, ExpressionInterface|string> $updateColumns
         */
        [$updates, $params] = $this->prepareUpdateSets($table, $updateColumns, $params);

        return $insertSql . ' ON DUPLICATE KEY UPDATE ' . implode(', ', $updates);
    }

    /**
     * Checks to see if the given limit is effective.
     *
     * @param mixed $limit the given limit.
     *
     * @return bool whether the limit is effective.
     */
    protected function hasLimit(mixed $limit): bool
    {
        /** In MySQL limit argument must be non-negative integer constant */
        return ctype_digit((string) $limit);
    }

    /**
     * Checks to see if the given offset is effective.
     *
     * @param mixed $offset the given offset.
     *
     * @return bool whether the offset is effective.
     */
    protected function hasOffset(mixed $offset): bool
    {
        /** In MySQL offset argument must be non-negative integer constant */
        $offset = (string) $offset;
        return ctype_digit($offset) && $offset !== '0';
    }

    /**
     * Prepares a `VALUES` part for an `INSERT` SQL statement.
     *
     * @param string $table the table that new rows will be inserted into.
     * @param array|Query $columns the column data (name => value) to be inserted into the table or instance of
     * {@see Query|Query} to perform INSERT INTO ... SELECT SQL statement.
     * @param array $params the binding parameters that will be generated by this method. They should be bound to the DB
     * command later.
     *
     * @throws Exception|InvalidArgumentException|InvalidConfigException|JsonException|NotSupportedException
     *
     * @return array array of column names, placeholders, values and params.
     */
    protected function prepareInsertValues(string $table, Query|array $columns, array $params = []): array
    {
        /**
         * @var array $names
         * @var array $placeholders
         */
        [$names, $placeholders, $values, $params] = parent::prepareInsertValues($table, $columns, $params);
        if (!$columns instanceof Query && empty($names)) {
            $tableSchema = $this->db->getSchema()->getTableSchema($table);

            if ($tableSchema !== null) {
                $columns = $tableSchema->getColumns();
                $columns = !empty($tableSchema->getPrimaryKey())
                    ? $tableSchema->getPrimaryKey() : [reset($columns)->getName()];
                /** @var string $name */
                foreach ($columns as $name) {
                    $names[] = $this->db->getQuoter()->quoteColumnName($name);
                    $placeholders[] = 'DEFAULT';
                }
            }
        }

        return [$names, $placeholders, $values, $params];
    }

    /**
     * Contains array of default expression builders. Extend this method and override it, if you want to change default
     * expression builders for this query builder.
     *
     * @return array
     *
     * See {@see ExpressionBuilder} docs for details.
     */
    protected function defaultExpressionBuilders(): array
    {
        return array_merge(
            parent::defaultExpressionBuilders(),
            [
                JsonExpression::class => JsonExpressionBuilder::class,
            ]
        );
    }

    /**
     * Returns the map for default time type.
     *
     * If the version of MySQL is lower than 5.6.4, then the types will be without fractional seconds, otherwise with
     * fractional seconds.
     *
     * @throws Exception
     *
     * @return array
     * @psalm-return array<string, string>
     */
    private function defaultTimeTypeMap(): array
    {
        $map = [
            SchemaPDOMysql::TYPE_DATETIME => 'datetime',
            SchemaPDOMysql::TYPE_TIMESTAMP => 'timestamp',
            SchemaPDOMysql::TYPE_TIME => 'time',
        ];

        if ($this->supportsFractionalSeconds()) {
            $map = [
                SchemaPDOMysql::TYPE_DATETIME => 'datetime(0)',
                SchemaPDOMysql::TYPE_TIMESTAMP => 'timestamp(0)',
                SchemaPDOMysql::TYPE_TIME => 'time(0)',
            ];
        }

        return $map;
    }

    /**
     * Gets column definition.
     *
     * @param string $table table name.
     * @param string $column column name.
     *
     * @throws Exception|Throwable in case when table does not contain column.
     *
     * @return string the column definition.
     */
    private function getColumnDefinition(string $table, string $column): string
    {
        $result = '';

        $quotedTable = $this->db->getQuoter()->quoteTableName($table);

        /** @var array<array-key, string> $row */
        $row = $this->db->createCommand('SHOW CREATE TABLE ' . $quotedTable)->queryOne();

        if (!isset($row['Create Table'])) {
            $row = array_values($row);
            $sql = $row[1];
        } else {
            $sql = $row['Create Table'];
        }

        if (preg_match_all('/^\s*`(.*?)`\s+(.*?),?$/m', $sql, $matches)) {
            foreach ($matches[1] as $i => $c) {
                if ($c === $column) {
                    $result = $matches[2][$i];
                }
            }
        }

        return $result;
    }

    /**
     * Checks the ability to use fractional seconds.
     *
     * @throws Exception
     *
     * {@see https://dev.mysql.com/doc/refman/5.6/en/fractional-seconds.html}
     *
     * @return bool
     */
    private function supportsFractionalSeconds(): bool
    {
        return version_compare($this->db->getServerVersion(), '5.6.4', '>=');
    }
}
