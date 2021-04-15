<?php
namespace binlogMessage\DbBinLog;

use MySQLReplication\Config\ConfigBuilder;
use MySQLReplication\Definitions\ConstEventType;
use MySQLReplication\Event\DTO\EventDTO;
use MySQLReplication\Event\EventSubscribers;
use MySQLReplication\MySQLReplicationFactory;
use Predis\Client;

class Subscriber extends EventSubscribers
{
    private $slaveId = null;
    private $configBuilder = null;
    private $binlogStream = null;
    private $redis = null;

    private $binLogFileName = null;
    private $binLogPosition = null;

    public $onMessage = null;

    function __construct()
    {
        $this->configBuilder = new ConfigBuilder();
    }


    public function recordLastRun(EventDTO $event)
    {
        if($this->redis)
        {
            //记录当前同步位置
            $this->redis->hMset('binlogSubscribe'.$this->slaveId, array(
                'binFileName' => $event->getEventInfo()->getBinLogCurrent()->getBinFileName(),
                'binLogPosition' => $event->getEventInfo()->getBinLogCurrent()->getBinLogPosition(),
            ));
        }

        return $this;
    }

    /**
     * @return $this
     * @throws \Exception
     *
     * TODO 加入切换 host 逻辑 不同 host binlog 日志不一致 -- DO
     */
    public function resumeLastRun()
    {
        if($this->redis)
        {
            //未设置 调用默认
            if(!$this->binLogFileName)
            {
                $lastRun = $this->redis->hGetAll('binlogSubscribe'.$this->slaveId);
                if($lastRun)
                {
                    $this->withBinLogFileName($lastRun['binFileName']);
                    $this->withBinLogPosition($lastRun['binLogPosition']);
                }
            }
        }
        return $this;
    }

    /**
     * @param null $parameters
     * @param null $options
     */
    public function withRedis($parameters = null, $options = null)
    {
        $this->redis = new Client($parameters,$options);
    }

    /**
     * @param string $user
     * @return ConfigBuilder
     */
    public function withUser($user)
    {
        $this->configBuilder->withUser($user);

        return $this->configBuilder;
    }

    /**
     * @param string $host
     * @return ConfigBuilder
     */
    public function withHost($host)
    {
        $this->configBuilder->withHost($host);

        return $this->configBuilder;
    }

    /**
     * @param int $port
     * @return ConfigBuilder
     */
    public function withPort($port)
    {
        $this->configBuilder->withPort($port);

        return $this->configBuilder;
    }

    /**
     * @param string $password
     * @return ConfigBuilder
     */
    public function withPassword($password)
    {
        $this->configBuilder->withPassword($password);

        return $this->configBuilder;
    }

    /**
     * @param string $charset
     * @return ConfigBuilder
     */
    public function withCharset($charset)
    {
        $this->configBuilder->withCharset($charset);

        return $this->configBuilder;
    }

    /**
     * @param string $gtid
     * @return ConfigBuilder
     */
    public function withGtid($gtid)
    {
        $this->configBuilder->withGtid($gtid);

        return $this->configBuilder;
    }

    /**
     * @param int $slaveId
     * @return ConfigBuilder
     */
    public function withSlaveId($slaveId)
    {
        $this->slaveId = $slaveId;
        $this->configBuilder->withSlaveId($slaveId);

        return $this->configBuilder;
    }

    /**
     * @param string $binLogFileName
     * @return ConfigBuilder
     */
    public function withBinLogFileName($binLogFileName = '')
    {
        $this->configBuilder->withBinLogFileName($binLogFileName);

        $this->binLogFileName = $binLogFileName;

        return $this->configBuilder;
    }

    /**
     * @param int $binLogPosition
     * @return ConfigBuilder
     */
    public function withBinLogPosition($binLogPosition = 0)
    {
        $this->configBuilder->withBinLogPosition($binLogPosition);
        $this->binLogPosition = $binLogPosition;

        return $this->configBuilder;
    }

    /**
     * @param array $eventsOnly
     * @return ConfigBuilder
     */
    public function withEventsOnly(array $eventsOnly)
    {
        $eventMap = array(
            'tableMap' => [ConstEventType::TABLE_MAP_EVENT],
            'rotate' => [ConstEventType::ROTATE_EVENT],
            'gtid' => [ConstEventType::GTID_LOG_EVENT],
            'heartbeat' => [ConstEventType::HEARTBEAT_LOG_EVENT],
            'mariadb gtid' => [ConstEventType::MARIA_GTID_EVENT],
            'update' => [ConstEventType::PRE_GA_UPDATE_ROWS_EVENT,ConstEventType::UPDATE_ROWS_EVENT_V1, ConstEventType::UPDATE_ROWS_EVENT_V2],
            'write' => [ConstEventType::PRE_GA_WRITE_ROWS_EVENT,ConstEventType::WRITE_ROWS_EVENT_V1, ConstEventType::WRITE_ROWS_EVENT_V2],
            'delete' => [ConstEventType::PRE_GA_DELETE_ROWS_EVENT,ConstEventType::DELETE_ROWS_EVENT_V1, ConstEventType::DELETE_ROWS_EVENT_V2],
            'xid' => [ConstEventType::XID_EVENT],
            'query' => [ConstEventType::QUERY_EVENT],
            'format description' => [ConstEventType::FORMAT_DESCRIPTION_EVENT],
        );

        $eventsOnlyT = array();
        if(!empty($eventsOnly))
        {
            foreach($eventsOnly as $event)
            {
                isset($eventMap[$event]) && $eventsOnlyT = array_merge($eventsOnlyT,$eventMap[$event]);
            }
        }
        $this->configBuilder->withEventsOnly($eventsOnlyT);

        return $this->configBuilder;
    }

    /**
     * @param array $eventsIgnore
     * @return ConfigBuilder
     */
    public function withEventsIgnore(array $eventsIgnore)
    {
        $this->configBuilder->withEventsIgnore($eventsIgnore);

        return $this->configBuilder;
    }

    /**
     * @param array $tablesOnly
     * @return ConfigBuilder
     */
    public function withTablesOnly(array $tablesOnly)
    {
        $this->configBuilder->withTablesOnly($tablesOnly);

        return $this->configBuilder;
    }

    /**
     * @param array $databasesOnly
     * @return ConfigBuilder
     */
    public function withDatabasesOnly(array $databasesOnly)
    {
        $this->configBuilder->withDatabasesOnly($databasesOnly);

        return $this->configBuilder;
    }

    /**
     * @param string $mariaDbGtid
     * @return ConfigBuilder
     */
    public function withMariaDbGtid($mariaDbGtid)
    {
        $this->configBuilder->withMariaDbGtid($mariaDbGtid);

        return $this->configBuilder;
    }

    /**
     * @param int $tableCacheSize
     */
    public function withTableCacheSize($tableCacheSize)
    {
        $this->configBuilder->withTableCacheSize($tableCacheSize);
    }

    protected function allEvents(EventDTO $event): void
    {
        $this->onMessage && call_user_func($this->onMessage,$event);
        $this->recordLastRun($event);
    }

    public function run()
    {
        if(!$this->slaveId) throw new \Exception('请设置 Mysql Slave Id');
        if(!$this->redis) throw new \Exception('请配置 redis 用于记录binlog解析位置');

        //恢复上次执行位置
        $this->resumeLastRun();

        $this->binlogStream = new MySQLReplicationFactory($this->configBuilder->build());
        $this->binlogStream->registerSubscriber($this);
        $this->binlogStream->run();
    }

    /**
     * 尝试其他方法丢 predis 实现
     *
     * @param $name
     * @param $arguments
     * @return mixed
     */
    public function __call($name, $arguments)
    {
        // TODO: Implement __call() method.
        if($this->redis)
        {
            return call_user_func_array(array($this->redis,$name),$arguments);
        }
    }

}