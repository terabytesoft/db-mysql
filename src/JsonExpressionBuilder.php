<?php

declare(strict_types=1);

namespace Yiisoft\Db\Mysql;

use JsonException;
use Yiisoft\Db\Exception\Exception;
use Yiisoft\Db\Exception\InvalidArgumentException;
use Yiisoft\Db\Exception\InvalidConfigException;
use Yiisoft\Db\Exception\NotSupportedException;
use Yiisoft\Db\Expression\ExpressionBuilderInterface;
use Yiisoft\Db\Expression\ExpressionInterface;
use Yiisoft\Db\Expression\JsonExpression;
use Yiisoft\Db\Query\Query;
use Yiisoft\Db\Query\QueryBuilderInterface;
use Yiisoft\Json\Json;

use function count;

/**
 * The class JsonExpressionBuilder builds {@see JsonExpression} for Mysql database.
 */
final class JsonExpressionBuilder implements ExpressionBuilderInterface
{
    public const PARAM_PREFIX = ':qp';

    public function __construct(private QueryBuilderInterface $queryBuilder)
    {
    }

    public function build(ExpressionInterface $expression, array &$params = []): string
    {
        /**
         * @var JsonExpression $expression
         * @var mixed|Query $value
         */
        $value = $expression->getValue();

        if ($value instanceof Query) {
            [$sql, $params] = $this->queryBuilder->build($value, $params);

            return "($sql)";
        }

        $placeholder = self::PARAM_PREFIX . count($params);
        $params[$placeholder] = Json::encode($value);

        return "CAST($placeholder AS JSON)";
    }
}
