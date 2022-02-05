<?php

declare(strict_types=1);

namespace Yiisoft\Db\Mysql\Tests;

use InvalidArgumentException;
use Yiisoft\Db\Mysql\DMLCommand;

/**
 * @group mysql
 */
final class DMLCommandTest extends TestCase
{
    public function testResetSequence(): void
    {
        $db = $this->getConnection(true);
        $dml = new DMLCommand($db->createCommand(), $db->getQuoter(), $db->getSchema());

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
        $dml = new DMLCommand($db->createCommand(), $db->getQuoter(), $db->getSchema());

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("There is no sequence associated with table 'constraints'");
        $sql = $dml->resetSequence('constraints');
    }

    public function testResetSequenceTableNoExist(): void
    {
        $db = $this->getConnection();
        $dml = new DMLCommand($db->createCommand(), $db->getQuoter(), $db->getSchema());

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Table not found: noExist');
        $sql = $dml->resetSequence('noExist', 1);
    }
}
