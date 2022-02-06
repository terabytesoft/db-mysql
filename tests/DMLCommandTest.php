<?php

declare(strict_types=1);

namespace Yiisoft\Db\Mysql\Tests;

use InvalidArgumentException;
use Yiisoft\Arrays\ArrayHelper;
use Yiisoft\Db\Expression\Expression;
use Yiisoft\Db\Expression\JsonExpression;
use Yiisoft\Db\Mysql\DMLCommand;
use Yiisoft\Db\Query\Query;

/**
 * @group mysql
 */
final class DMLCommandTest extends TestCase
{
    public function insertProvider(): array
    {
        return [
            'regular-values' => [
                'customer',
                [
                    'email' => 'test@example.com',
                    'name' => 'silverfire',
                    'address' => 'Kyiv {{city}}, Ukraine',
                    'is_active' => false,
                    'related_id' => null,
                ],
                [],
                $this->replaceQuotes(
                    'INSERT INTO [[customer]] ([[email]], [[name]], [[address]], [[is_active]], [[related_id]])'
                    . ' VALUES (:qp0, :qp1, :qp2, :qp3, :qp4)'
                ),
                [
                    ':qp0' => 'test@example.com',
                    ':qp1' => 'silverfire',
                    ':qp2' => 'Kyiv {{city}}, Ukraine',
                    ':qp3' => false,
                    ':qp4' => null,
                ],
            ],
            'params-and-expressions' => [
                '{{%type}}',
                [
                    '{{%type}}.[[related_id]]' => null,
                    '[[time]]' => new Expression('now()'),
                ],
                [],
                'INSERT INTO {{%type}} ({{%type}}.[[related_id]], [[time]]) VALUES (:qp0, now())',
                [
                    ':qp0' => null,
                ],
            ],
            'carry passed params' => [
                'customer',
                [
                    'email' => 'test@example.com',
                    'name' => 'sergeymakinen',
                    'address' => '{{city}}',
                    'is_active' => false,
                    'related_id' => null,
                    'col' => new Expression('CONCAT(:phFoo, :phBar)', [':phFoo' => 'foo']),
                ],
                [':phBar' => 'bar'],
                $this->replaceQuotes(
                    'INSERT INTO [[customer]] ([[email]], [[name]], [[address]], [[is_active]], [[related_id]], [[col]])'
                    . ' VALUES (:qp1, :qp2, :qp3, :qp4, :qp5, CONCAT(:phFoo, :phBar))'
                ),
                [
                    ':phBar' => 'bar',
                    ':qp1' => 'test@example.com',
                    ':qp2' => 'sergeymakinen',
                    ':qp3' => '{{city}}',
                    ':qp4' => false,
                    ':qp5' => null,
                    ':phFoo' => 'foo',
                ],
            ],
            'carry passed params (query)' => [
                'customer',
                (new Query($this->getConnection()))
                    ->select([
                        'email',
                        'name',
                        'address',
                        'is_active',
                        'related_id',
                    ])
                    ->from('customer')
                    ->where([
                        'email' => 'test@example.com',
                        'name' => 'sergeymakinen',
                        'address' => '{{city}}',
                        'is_active' => false,
                        'related_id' => null,
                        'col' => new Expression('CONCAT(:phFoo, :phBar)', [':phFoo' => 'foo']),
                    ]),
                [':phBar' => 'bar'],
                $this->replaceQuotes(
                    'INSERT INTO [[customer]] ([[email]], [[name]], [[address]], [[is_active]], [[related_id]])'
                    . ' SELECT [[email]], [[name]], [[address]], [[is_active]], [[related_id]] FROM [[customer]]'
                    . ' WHERE ([[email]]=:qp1) AND ([[name]]=:qp2) AND ([[address]]=:qp3) AND ([[is_active]]=:qp4)'
                    . ' AND ([[related_id]] IS NULL) AND ([[col]]=CONCAT(:phFoo, :phBar))'
                ),
                [
                    ':phBar' => 'bar',
                    ':qp1' => 'test@example.com',
                    ':qp2' => 'sergeymakinen',
                    ':qp3' => '{{city}}',
                    ':qp4' => false,
                    ':phFoo' => 'foo',
                ],
            ],
        ];
    }

    /**
     * @dataProvider insertProvider
     *
     * @param string $table
     * @param array|ColumnSchema $columns
     * @param array $params
     * @param string|null $expectedSQL
     * @param array $expectedParams
     *
     * @throws Exception
     * @throws InvalidArgumentException
     * @throws InvalidConfigException
     * @throws NotSupportedException
     */
    public function testInsert(string $table, $columns, array $params, ?string $expectedSQL, array $expectedParams): void
    {
        $db = $this->getConnection();
        $dml = new DMLCommand($db->createCommand(), $db->getQueryBuilder(), $db->getQuoter(), $db->getSchema());
        $actualParams = $params;
        $actualSQL = $dml->insert($table, $columns, $actualParams);
        $this->assertSame($expectedSQL, $actualSQL);
        $this->assertSame($expectedParams, $actualParams);
    }

    public function testResetSequence(): void
    {
        $db = $this->getConnection(true);
        $dml = new DMLCommand($db->createCommand(), $db->getQueryBuilder(), $db->getQuoter(), $db->getSchema());

        $expected = 'ALTER TABLE `item` AUTO_INCREMENT=6';
        $sql = $dml->resetSequence('item');
        $this->assertSame($expected, $sql);

        $expected = 'ALTER TABLE `item` AUTO_INCREMENT=4';
        $sql = $dml->resetSequence('item', 4);
        $this->assertSame($expected, $sql);
    }

    public function testResetSequenceNoAssociated(): void
    {
        $db = $this->getConnection();
        $dml = new DMLCommand($db->createCommand(), $db->getQueryBuilder(), $db->getQuoter(), $db->getSchema());

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("There is no sequence associated with table 'constraints'");
        $sql = $dml->resetSequence('constraints');
    }

    public function testResetSequenceTableNoExist(): void
    {
        $db = $this->getConnection();
        $dml = new DMLCommand($db->createCommand(), $db->getQueryBuilder(), $db->getQuoter(), $db->getSchema());

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Table not found: noExist');
        $sql = $dml->resetSequence('noExist', 1);
    }

    public function updateProvider(): array
    {
        $items = $this->updateProviderTrait();
        $items[] = [
            'profile',
            [
                'description' => new JsonExpression(['abc' => 'def', 123, null]),
            ],
            [
                'id' => 1,
            ],
            $this->replaceQuotes('UPDATE [[profile]] SET [[description]]=CAST(:qp0 AS JSON) WHERE [[id]]=:qp1'),
            [
                ':qp0' => '{"abc":"def","0":123,"1":null}',
                ':qp1' => 1,
            ],
        ];
        return $items;
    }

    public function updateProviderTrait(): array
    {
        return [
            [
                'customer',
                [
                    'status' => 1,
                    'updated_at' => new Expression('now()'),
                ],
                [
                    'id' => 100,
                ],
                $this->replaceQuotes(
                    'UPDATE [[customer]] SET [[status]]=:qp0, [[updated_at]]=now() WHERE [[id]]=:qp1'
                ),
                [
                    ':qp0' => 1,
                    ':qp1' => 100,
                ],
            ],
        ];
    }

    /**
     * @dataProvider updateProvider
     *
     * @param string $table
     * @param array $columns
     * @param array|string $condition
     * @param string|null $expectedSQL
     * @param array $expectedParams
     *
     * @throws Exception
     * @throws InvalidArgumentException
     * @throws InvalidConfigException
     * @throws NotSupportedException
     */
    public function testUpdate(
        string $table,
        array $columns,
        $condition,
        ?string $expectedSQL,
        array $expectedParams
    ): void {
        $actualParams = [];
        $db = $this->getConnection();
        $dml = new DMLCommand($db->createCommand(), $db->getQueryBuilder(), $db->getQuoter(), $db->getSchema());
        $actualSQL = $dml->update($table, $columns, $condition, $actualParams);
        $this->assertSame($expectedSQL, $actualSQL);
        $this->assertSame($expectedParams, $actualParams);
    }

    public function upsertProviderTrait(): array
    {
        return [
            'regular values' => [
                'T_upsert',
                [
                    'email' => 'test@example.com',
                    'address' => 'bar {{city}}',
                    'status' => 1,
                    'profile_id' => null,
                ],
                true,
                null,
                [
                    ':qp0' => 'test@example.com',
                    ':qp1' => 'bar {{city}}',
                    ':qp2' => 1,
                    ':qp3' => null,
                ],
            ],
            'regular values with update part' => [
                'T_upsert',
                [
                    'email' => 'test@example.com',
                    'address' => 'bar {{city}}',
                    'status' => 1,
                    'profile_id' => null,
                ],
                [
                    'address' => 'foo {{city}}',
                    'status' => 2,
                    'orders' => new Expression('T_upsert.orders + 1'),
                ],
                null,
                [
                    ':qp0' => 'test@example.com',
                    ':qp1' => 'bar {{city}}',
                    ':qp2' => 1,
                    ':qp3' => null,
                    ':qp4' => 'foo {{city}}',
                    ':qp5' => 2,
                ],
            ],
            'regular values without update part' => [
                'T_upsert',
                [
                    'email' => 'test@example.com',
                    'address' => 'bar {{city}}',
                    'status' => 1,
                    'profile_id' => null,
                ],
                false,
                null,
                [
                    ':qp0' => 'test@example.com',
                    ':qp1' => 'bar {{city}}',
                    ':qp2' => 1,
                    ':qp3' => null,
                ],
            ],
            'query' => [
                'T_upsert',
                (new Query($this->getConnection()))
                    ->select([
                        'email',
                        'status' => new Expression('2'),
                    ])
                    ->from('customer')
                    ->where(['name' => 'user1'])
                    ->limit(1),
                true,
                null,
                [
                    ':qp0' => 'user1',
                ],
            ],
            'query with update part' => [
                'T_upsert',
                (new Query($this->getConnection()))
                    ->select([
                        'email',
                        'status' => new Expression('2'),
                    ])
                    ->from('customer')
                    ->where(['name' => 'user1'])
                    ->limit(1),
                [
                    'address' => 'foo {{city}}',
                    'status' => 2,
                    'orders' => new Expression('T_upsert.orders + 1'),
                ],
                null,
                [
                    ':qp0' => 'user1',
                    ':qp1' => 'foo {{city}}',
                    ':qp2' => 2,
                ],
            ],
            'query without update part' => [
                'T_upsert',
                (new Query($this->getConnection()))
                    ->select([
                        'email',
                        'status' => new Expression('2'),
                    ])
                    ->from('customer')
                    ->where(['name' => 'user1'])
                    ->limit(1),
                false,
                null,
                [
                    ':qp0' => 'user1',
                ],
            ],
            'values and expressions' => [
                '{{%T_upsert}}',
                [
                    '{{%T_upsert}}.[[email]]' => 'dynamic@example.com',
                    '[[ts]]' => new Expression('now()'),
                ],
                true,
                null,
                [
                    ':qp0' => 'dynamic@example.com',
                ],
            ],
            'values and expressions with update part' => [
                '{{%T_upsert}}',
                [
                    '{{%T_upsert}}.[[email]]' => 'dynamic@example.com',
                    '[[ts]]' => new Expression('now()'),
                ],
                [
                    '[[orders]]' => new Expression('T_upsert.orders + 1'),
                ],
                null,
                [
                    ':qp0' => 'dynamic@example.com',
                ],
            ],
            'values and expressions without update part' => [
                '{{%T_upsert}}',
                [
                    '{{%T_upsert}}.[[email]]' => 'dynamic@example.com',
                    '[[ts]]' => new Expression('now()'),
                ],
                false,
                null,
                [
                    ':qp0' => 'dynamic@example.com',
                ],
            ],
            'query, values and expressions with update part' => [
                '{{%T_upsert}}',
                (new Query($this->getConnection()))
                    ->select([
                        'email' => new Expression(':phEmail', [':phEmail' => 'dynamic@example.com']),
                        '[[time]]' => new Expression('now()'),
                    ]),
                [
                    'ts' => 0,
                    '[[orders]]' => new Expression('T_upsert.orders + 1'),
                ],
                null,
                [
                    ':phEmail' => 'dynamic@example.com',
                    ':qp1' => 0,
                ],
            ],
            'query, values and expressions without update part' => [
                '{{%T_upsert}}',
                (new Query($this->getConnection()))
                    ->select([
                        'email' => new Expression(':phEmail', [':phEmail' => 'dynamic@example.com']),
                        '[[time]]' => new Expression('now()'),
                    ]),
                [
                    'ts' => 0,
                    '[[orders]]' => new Expression('T_upsert.orders + 1'),
                ],
                null,
                [
                    ':phEmail' => 'dynamic@example.com',
                    ':qp1' => 0,
                ],
            ],
            'no columns to update' => [
                'T_upsert_1',
                [
                    'a' => 1,
                ],
                false,
                null,
                [
                    ':qp0' => 1,
                ],
            ],
        ];
    }

    public function upsertProvider(): array
    {
        $concreteData = [
            'regular values' => [
                3 => 'INSERT INTO `T_upsert` (`email`, `address`, `status`, `profile_id`) VALUES (:qp0, :qp1, :qp2, :qp3) ON DUPLICATE KEY UPDATE `address`=VALUES(`address`), `status`=VALUES(`status`), `profile_id`=VALUES(`profile_id`)',
            ],
            'regular values with update part' => [
                3 => 'INSERT INTO `T_upsert` (`email`, `address`, `status`, `profile_id`) VALUES (:qp0, :qp1, :qp2, :qp3) ON DUPLICATE KEY UPDATE `address`=:qp4, `status`=:qp5, `orders`=T_upsert.orders + 1',
            ],
            'regular values without update part' => [
                3 => 'INSERT INTO `T_upsert` (`email`, `address`, `status`, `profile_id`) VALUES (:qp0, :qp1, :qp2, :qp3) ON DUPLICATE KEY UPDATE `email`=`T_upsert`.`email`',
            ],
            'query' => [
                3 => 'INSERT INTO `T_upsert` (`email`, `status`) SELECT `email`, 2 AS `status` FROM `customer` WHERE `name`=:qp0 LIMIT 1 ON DUPLICATE KEY UPDATE `status`=VALUES(`status`)',
            ],
            'query with update part' => [
                3 => 'INSERT INTO `T_upsert` (`email`, `status`) SELECT `email`, 2 AS `status` FROM `customer` WHERE `name`=:qp0 LIMIT 1 ON DUPLICATE KEY UPDATE `address`=:qp1, `status`=:qp2, `orders`=T_upsert.orders + 1',
            ],
            'query without update part' => [
                3 => 'INSERT INTO `T_upsert` (`email`, `status`) SELECT `email`, 2 AS `status` FROM `customer` WHERE `name`=:qp0 LIMIT 1 ON DUPLICATE KEY UPDATE `email`=`T_upsert`.`email`',
            ],
            'values and expressions' => [
                3 => 'INSERT INTO {{%T_upsert}} ({{%T_upsert}}.[[email]], [[ts]]) VALUES (:qp0, now())',
            ],
            'values and expressions with update part' => [
                3 => 'INSERT INTO {{%T_upsert}} ({{%T_upsert}}.[[email]], [[ts]]) VALUES (:qp0, now())',
            ],
            'values and expressions without update part' => [
                3 => 'INSERT INTO {{%T_upsert}} ({{%T_upsert}}.[[email]], [[ts]]) VALUES (:qp0, now())',
            ],
            'query, values and expressions with update part' => [
                3 => 'INSERT INTO {{%T_upsert}} (`email`, [[time]]) SELECT :phEmail AS `email`, now() AS [[time]] ON DUPLICATE KEY UPDATE `ts`=:qp1, [[orders]]=T_upsert.orders + 1',
            ],
            'query, values and expressions without update part' => [
                3 => 'INSERT INTO {{%T_upsert}} (`email`, [[time]]) SELECT :phEmail AS `email`, now() AS [[time]] ON DUPLICATE KEY UPDATE `ts`=:qp1, [[orders]]=T_upsert.orders + 1',
            ],
            'no columns to update' => [
                3 => 'INSERT INTO `T_upsert_1` (`a`) VALUES (:qp0) ON DUPLICATE KEY UPDATE `a`=`T_upsert_1`.`a`',
            ],
        ];

        $newData = $this->upsertProviderTrait();

        foreach ($concreteData as $testName => $data) {
            $newData[$testName] = \array_replace($newData[$testName], $data);
        }

        return $newData;
    }

    /**
     * @dataProvider upsertProvider
     *
     * @param string $table
     * @param array|ColumnSchema $insertColumns
     * @param array|bool|null $updateColumns
     * @param string|string[] $expectedSQL
     * @param array $expectedParams
     *
     * @throws NotSupportedException
     * @throws Exception
     */
    public function testUpsert(string $table, $insertColumns, $updateColumns, $expectedSQL, array $expectedParams): void
    {
        $actualParams = [];
        $db = $this->getConnection();
        $dml = new DMLCommand($db->createCommand(), $db->getQueryBuilder(), $db->getQuoter(), $db->getSchema());
        $actualSQL = $dml->upsert($table, $insertColumns, $updateColumns, $actualParams);

        if (is_string($expectedSQL)) {
            $this->assertSame($expectedSQL, $actualSQL);
        } else {
            $this->assertContains($actualSQL, $expectedSQL);
        }

        if (ArrayHelper::isAssociative($expectedParams)) {
            $this->assertSame($expectedParams, $actualParams);
        } else {
            $this->assertIsOneOf($actualParams, $expectedParams);
        }
    }
}
