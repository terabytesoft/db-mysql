<?php

declare(strict_types=1);

namespace Yiisoft\Db\Mysql\Tests;

use Closure;
use Yiisoft\Arrays\ArrayHelper;
use Yiisoft\Db\Connection\ConnectionInterface;
use Yiisoft\Db\Exception\Exception;
use Yiisoft\Db\Exception\IntegrityException;
use Yiisoft\Db\Exception\InvalidArgumentException;
use Yiisoft\Db\Exception\InvalidConfigException;
use Yiisoft\Db\Exception\NotSupportedException;
use Yiisoft\Db\Expression\Expression;
use Yiisoft\Db\Expression\JsonExpression;
use Yiisoft\Db\Mysql\ColumnSchema;
use Yiisoft\Db\Mysql\PDO\QueryBuilderPDOMysql;
use Yiisoft\Db\Query\Query;
use Yiisoft\Db\Query\QueryBuilderInterface;
use Yiisoft\Db\TestSupport\TestQueryBuilderTrait;

use function array_merge;
use function is_string;

/**
 * @group mysql
 */
final class QueryBuilderTest extends TestCase
{
    use TestQueryBuilderTrait;

    /**
     * @param bool $reset
     *
     * @return QueryBuilderInterface
     */
    protected function getQueryBuilder(ConnectionInterface $db): QueryBuilderInterface
    {
        return new QueryBuilderPDOMysql($db);
    }

    public function addDropForeignKeysProvider(): array
    {
        $result = $this->addDropforeignKeysProviderTrait();
        $result['drop'][0] = 'ALTER TABLE {{T_constraints_3}} DROP FOREIGN KEY [[CN_constraints_3]]';
        return $result;
    }

    /**
     * @dataProvider addDropForeignKeysProvider
     *
     * @param string $sql
     * @param Closure $builder
     */
    public function testAddDropForeignKey(string $sql, Closure $builder): void
    {
        $db = $this->getConnection();
        $this->assertSame($db->getQuoter()->quoteSql($sql), $builder($this->getQueryBuilder($db)));
    }

    public function addDropPrimaryKeysProvider(): array
    {
        $result = $this->addDropPrimaryKeysProviderTrait();

        $result['drop'][0] = 'ALTER TABLE {{T_constraints_1}} DROP PRIMARY KEY';
        $result['add'][0] = 'ALTER TABLE {{T_constraints_1}} ADD CONSTRAINT [[CN_pk]] PRIMARY KEY ([[C_id_1]])';
        $result['add (2 columns)'][0] = 'ALTER TABLE {{T_constraints_1}} ADD CONSTRAINT [[CN_pk]]'
            . ' PRIMARY KEY ([[C_id_1]], [[C_id_2]])';

        return $result;
    }

    /**
     * @dataProvider addDropPrimaryKeysProvider
     *
     * @param string $sql
     * @param Closure $builder
     */
    public function testAddDropPrimaryKey(string $sql, Closure $builder): void
    {
        $db = $this->getConnection();
        $this->assertSame($db->getQuoter()->quoteSql($sql), $builder($this->getQueryBuilder($db)));
    }

    public function addDropUniquesProvider(): array
    {
        $result = $this->addDropUniquesProviderTrait();
        $result['drop'][0] = 'DROP INDEX [[CN_unique]] ON {{T_constraints_1}}';
        return $result;
    }

    /**
     * @dataProvider addDropUniquesProvider
     *
     * @param string $sql
     * @param Closure $builder
     */
    public function testAddDropUnique(string $sql, Closure $builder): void
    {
        $db = $this->getConnection();
        $this->assertSame($db->getQuoter()->quoteSql($sql), $builder($this->getQueryBuilder($db)));
    }

    /**
     * @dataProvider batchInsertProviderTrait
     *
     * @param string $table
     * @param array $columns
     * @param array $value
     * @param string|null $expected
     *
     * @throws Exception
     * @throws InvalidArgumentException
     * @throws InvalidConfigException
     * @throws NotSupportedException
     */
    public function testBatchInsert(string $table, array $columns, array $value, ?string $expected): void
    {
        $db = $this->getConnection();
        $qb = $this->getQueryBuilder($db);
        $sql = $qb->batchInsert($table, $columns, $value);
        $this->assertEquals($expected, $sql);
    }

    public function buildConditionsProvider(): array
    {
        return array_merge($this->buildConditionsProviderTrait(), [
            [
                ['=', 'jsoncol', new JsonExpression(['lang' => 'uk', 'country' => 'UA'])],
                '[[jsoncol]] = CAST(:qp0 AS JSON)', [':qp0' => '{"lang":"uk","country":"UA"}'],
            ],
            [
                ['=', 'jsoncol', new JsonExpression([false])],
                '[[jsoncol]] = CAST(:qp0 AS JSON)', [':qp0' => '[false]'],
            ],
            'object with type. Type is ignored for MySQL' => [
                ['=', 'prices', new JsonExpression(['seeds' => 15, 'apples' => 25], 'jsonb')],
                '[[prices]] = CAST(:qp0 AS JSON)', [':qp0' => '{"seeds":15,"apples":25}'],
            ],
            'nested json' => [
                [
                    '=',
                    'data',
                    new JsonExpression(
                        [
                            'user' => ['login' => 'silverfire', 'password' => 'c4ny0ur34d17?'],
                            'props' => ['mood' => 'good'],
                        ]
                    ),
                ],
                '[[data]] = CAST(:qp0 AS JSON)',
                [':qp0' => '{"user":{"login":"silverfire","password":"c4ny0ur34d17?"},"props":{"mood":"good"}}'],
            ],
            'null value' => [
                ['=', 'jsoncol', new JsonExpression(null)],
                '[[jsoncol]] = CAST(:qp0 AS JSON)', [':qp0' => 'null'],
            ],
            'null as array value' => [
                ['=', 'jsoncol', new JsonExpression([null])],
                '[[jsoncol]] = CAST(:qp0 AS JSON)', [':qp0' => '[null]'],
            ],
            'null as object value' => [
                ['=', 'jsoncol', new JsonExpression(['nil' => null])],
                '[[jsoncol]] = CAST(:qp0 AS JSON)', [':qp0' => '{"nil":null}'],
            ],
            'query' => [
                [
                    '=',
                    'jsoncol',
                    new JsonExpression((new Query($this->getConnection()))->select('params')->from('user')->where(['id' => 1])),
                ],
                '[[jsoncol]] = (SELECT [[params]] FROM [[user]] WHERE [[id]]=:qp0)',
                [':qp0' => 1],
            ],
            'query with type, that is ignored in MySQL' => [
                [
                    '=',
                    'jsoncol',
                    new JsonExpression((new Query($this->getConnection()))->select('params')->from('user')->where(['id' => 1]), 'jsonb'),
                ],
                '[[jsoncol]] = (SELECT [[params]] FROM [[user]] WHERE [[id]]=:qp0)', [':qp0' => 1],
            ],
            'nested and combined json expression' => [
                [
                    '=',
                    'jsoncol',
                    new JsonExpression(new JsonExpression(['a' => 1, 'b' => 2, 'd' => new JsonExpression(['e' => 3])])),
                ],
                '[[jsoncol]] = CAST(:qp0 AS JSON)', [':qp0' => '{"a":1,"b":2,"d":{"e":3}}'],
            ],
            'search by property in JSON column (issue #15838)' => [
                ['=', new Expression("(jsoncol->>'$.someKey')"), '42'],
                "(jsoncol->>'$.someKey') = :qp0", [':qp0' => 42],
            ],
        ]);
    }

    /**
     * @dataProvider buildConditionsProvider
     *
     * @param array|ExpressionInterface $condition
     * @param string|null $expected
     * @param array $expectedParams
     *
     * @throws Exception
     * @throws InvalidArgumentException
     * @throws InvalidConfigException
     * @throws NotSupportedException
     */
    public function testBuildCondition($condition, ?string $expected, array $expectedParams): void
    {
        $db = $this->getConnection();
        $query = (new Query($db))->where($condition);
        [$sql, $params] = $this->getQueryBuilder($db)->build($query);
        $this->assertEquals('SELECT *' . (empty($expected) ? '' : ' WHERE ' . $this->replaceQuotes($expected)), $sql);
        $this->assertEquals($expectedParams, $params);
    }

    /**
     * @dataProvider buildFilterConditionProviderTrait
     *
     * @param array $condition
     * @param string|null $expected
     * @param array $expectedParams
     *
     * @throws Exception
     * @throws InvalidArgumentException
     * @throws InvalidConfigException
     * @throws NotSupportedException
     */
    public function testBuildFilterCondition(array $condition, ?string $expected, array $expectedParams): void
    {
        $db = $this->getConnection();
        $query = (new Query($db))->filterWhere($condition);
        [$sql, $params] = $this->getQueryBuilder($db)->build($query);
        $this->assertEquals('SELECT *' . (empty($expected) ? '' : ' WHERE ' . $this->replaceQuotes($expected)), $sql);
        $this->assertEquals($expectedParams, $params);
    }

    /**
     * @dataProvider buildFromDataProviderTrait
     *
     * @param string $table
     * @param string $expected
     *
     * @throws Exception
     */
    public function testBuildFrom(string $table, string $expected): void
    {
        $db = $this->getConnection();
        $params = [];
        $sql = $this->getQueryBuilder($db)->buildFrom([$table], $params);
        $this->assertEquals('FROM ' . $this->replaceQuotes($expected), $sql);
    }

    /**
     * @dataProvider buildLikeConditionsProviderTrait
     *
     * @param array|object $condition
     * @param string|null $expected
     * @param array $expectedParams
     *
     * @throws Exception
     * @throws InvalidArgumentException
     * @throws InvalidConfigException
     * @throws NotSupportedException
     */
    public function testBuildLikeCondition($condition, ?string $expected, array $expectedParams): void
    {
        $db = $this->getConnection();
        $query = (new Query($db))->where($condition);
        [$sql, $params] = $this->getQueryBuilder($db)->build($query);
        $this->assertEquals('SELECT *' . (empty($expected) ? '' : ' WHERE ' . $this->replaceQuotes($expected)), $sql);
        $this->assertEquals($expectedParams, $params);
    }

    /**
     * @dataProvider buildExistsParamsProviderTrait
     *
     * @param string $cond
     * @param string|null $expectedQuerySql
     *
     * @throws Exception
     * @throws InvalidArgumentException
     * @throws InvalidConfigException
     * @throws NotSupportedException
     */
    public function testBuildWhereExists(string $cond, ?string $expectedQuerySql): void
    {
        $db = $this->getConnection();
        $expectedQueryParams = [];
        $subQuery = new Query($db);
        $subQuery->select('1')->from('Website w');
        $query = new Query($db);
        $query->select('id')->from('TotalExample t')->where([$cond, $subQuery]);
        [$actualQuerySql, $actualQueryParams] = $this->getQueryBuilder($db)->build($query);
        $this->assertEquals($expectedQuerySql, $actualQuerySql);
        $this->assertEquals($expectedQueryParams, $actualQueryParams);
    }

    public function createDropIndexesProvider(): array
    {
        $result = $this->createDropIndexesProviderTrait();

        $result['create'][0] = 'ALTER TABLE {{T_constraints_2}} ADD INDEX [[CN_constraints_2_single]] ([[C_index_1]])';

        $result['create (2 columns)'][0] = 'ALTER TABLE {{T_constraints_2}} ADD INDEX [[CN_constraints_2_multi]]'
            . ' ([[C_index_2_1]], [[C_index_2_2]])';

        $result['create unique'][0] = 'ALTER TABLE {{T_constraints_2}} ADD UNIQUE INDEX [[CN_constraints_2_single]]'
            . ' ([[C_index_1]])';

        $result['create unique (2 columns)'][0] = 'ALTER TABLE {{T_constraints_2}} ADD UNIQUE'
            . ' INDEX [[CN_constraints_2_multi]] ([[C_index_2_1]], [[C_index_2_2]])';

        return $result;
    }

    /**
     * @dataProvider createDropIndexesProvider
     *
     * @param string $sql
     */
    public function testCreateDropIndex(string $sql, Closure $builder): void
    {
        $db = $this->getConnection();
        $this->assertSame($db->getQuoter()->quoteSql($sql), $builder($this->getQueryBuilder($db)));
    }

    /**
     * @dataProvider deleteProviderTrait
     *
     * @param string $table
     * @param array|string $condition
     * @param string|null $expectedSQL
     * @param array $expectedParams
     *
     * @throws Exception
     * @throws InvalidArgumentException
     * @throws InvalidConfigException
     * @throws NotSupportedException
     */
    public function testDelete(string $table, $condition, ?string $expectedSQL, array $expectedParams): void
    {
        $db = $this->getConnection();
        $actualParams = [];
        $actualSQL = $this->getQueryBuilder($db)->delete($table, $condition, $actualParams);
        $this->assertSame($expectedSQL, $actualSQL);
        $this->assertSame($expectedParams, $actualParams);
    }

    public function testRenameColumn(): void
    {
        $db = $this->getConnection();
        $qb = $this->getQueryBuilder($db);

        $sql = $qb->renameColumn('alpha', 'string_identifier', 'string_identifier_test');
        $this->assertSame(
            'ALTER TABLE `alpha` CHANGE `string_identifier` `string_identifier_test` varchar(255) NOT NULL',
            $sql,
        );

        $sql = $qb->renameColumn('alpha', 'string_identifier_test', 'string_identifier');
        $this->assertSame(
            'ALTER TABLE `alpha` CHANGE `string_identifier_test` `string_identifier`',
            $sql,
        );
    }

    public function testRenameColumnTableNoExist(): void
    {
        $db = $this->getConnection();
        $qb = $this->getQueryBuilder($db);
        $this->expectException(Exception::class);
        $sql = $qb->renameColumn('noExist', 'string_identifier', 'string_identifier_test');
    }

    public function testCheckIntegrity(): void
    {
        $db = $this->getConnection();
        $db->createCommand()->checkIntegrity('public', 'item', false)->execute();
        $sql = 'INSERT INTO {{item}}([[name]], [[category_id]]) VALUES (\'invalid\', 99999)';
        $command = $db->createCommand($sql);
        $command->execute();
        $db->createCommand()->checkIntegrity('public', 'item', true)->execute();
        $this->expectException(IntegrityException::class);
        $command->execute();
    }

    public function testCommentTable(): void
    {
        $db = $this->getConnection();
        $qb = $this->getQueryBuilder($db);

        $expected = "ALTER TABLE [[comment]] COMMENT 'This is my table.'";
        $sql = $qb->addCommentOnTable('comment', 'This is my table.');
        $this->assertEquals($this->replaceQuotes($expected), $sql);

        $expected = "ALTER TABLE [[comment]] COMMENT ''";
        $sql = $qb->dropCommentFromTable('comment');
        $this->assertEquals($this->replaceQuotes($expected), $sql);
    }

    public function testCommentColumn(): void
    {
        $db = $this->getConnection();
        $qb = $this->getQueryBuilder($db);

        $expected = 'ALTER TABLE [[comment]] CHANGE [[add_comment]] [[add_comment]]' .
            " varchar(255) NOT NULL COMMENT 'This is my column.'";
        $sql = $qb->addCommentOnColumn('comment', 'add_comment', 'This is my column.');
        $this->assertEquals($this->replaceQuotes($expected), $sql);

        $expected = 'ALTER TABLE [[comment]] CHANGE [[replace_comment]] [[replace_comment]]' .
            " varchar(255) DEFAULT NULL COMMENT 'This is my column.'";
        $sql = $qb->addCommentOnColumn('comment', 'replace_comment', 'This is my column.');
        $this->assertEquals($this->replaceQuotes($expected), $sql);

        $expected = 'ALTER TABLE [[comment]] CHANGE [[delete_comment]] [[delete_comment]]' .
            " varchar(128) NOT NULL COMMENT ''";
        $sql = $qb->dropCommentFromColumn('comment', 'delete_comment');
        $this->assertEquals($this->replaceQuotes($expected), $sql);
    }

    public function testDropCheck(): void
    {
        $db = $this->getConnection();
        $qb = $this->getQueryBuilder($db);
        $this->expectException(NotSupportedException::class);
        $this->expectExceptionMessage(
            'Yiisoft\Db\Mysql\PDO\QueryBuilderPDOMysql::dropCheck is not supported by MySQL.'
        );
        $qb->dropCheck('noExist', 'noExist');
    }
}
