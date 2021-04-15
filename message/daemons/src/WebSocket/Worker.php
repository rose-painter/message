<?php
namespace Message\WebSocket;

class Worker
{
    const STATUS_STARTING = 1;
    const STATUS_RUNNING = 2;
    const STATUS_SHUTDOWN = 4;
    const STATUS_RELOADING = 8;

    public static $arguments = [];
    protected static $uniqueName = '';
    public static $logDir = '/var/log';
    public static $logFile = null;
    public static $logRotate = 86400; //日志轮换时间
    protected static $_logRotateTime = 0; //上次日志轮换时间
    public static $pidDir = '/var/run';
    public static $pidFile = null;
    public static $stdoutFile = '/dev/null';
    public static $runAsUser = 'npc'; //设置进程允许在某个用户模式下
    protected static $_startTime = 0; //任务开始时间

    protected static $_pid = null;
    protected static $_masterPid = null;

    //worker 相关的变量
    public static $index = 0;
    public static $workerNum = 1;
    protected static $_workers = [];
    protected static $_job = null;
    protected static $_status = self::STATUS_STARTING;

    public function __construct()
    {
    }

    public function run()
    {
        static::init();
        static::parseArguments();
        static::daemon();
        static::registerSignalHandlerMaster();
        static::forkWorkers();
        static::registerShutdownHandler();
        static::monitorWorkers();
    }

    /**
     * 初始化
     */
    protected static function init()
    {
        $backtrace        = \debug_backtrace();

        //获取执行脚本名
        static::$uniqueName = basename($backtrace[\count($backtrace) - 1]['file'],'.php');

        if(!\is_file(static::getLogFile()))
        {
            \touch(static::getLogFile());
            \chmod(static::getLogFile(),0622);
        }

    }

    /**
     * 初始化命令行参数
     *
     * TODO 如果需要对全局参数进行覆盖 可以在此进行处理
     *
     */
    protected static function parseArguments()
    {
        global $argv;

        foreach($argv as $k => $v)
        {
            if(trim($v,'-') !== $v)
            {
                $v = trim($v,'-');
                //TODO 此处 $param 变量定义了 空参的默认行为 --stop 等同于 --stop=true
                $param = true;
                if(stripos($v,'=') !== false)
                {
                    list($v,$param) = explode('=',$v);
                }
                static::$arguments[$v] = isset($argv[$k+1]) && stripos($argv[$k+1],'-') === false ? $argv[$k+1] : $param;
            }
        }

        if(isset(static::$arguments['logFile']))
        {
            static::$logFile = static::$arguments['logFile'];
        }

        if(isset(static::$arguments['pidFile']))
        {
            static::$pidFile = static::$arguments['pidFile'];
        }

        //特殊命令处理 TODO
        $pid = @file_get_contents(static::getPidFile());
        //检查进程是否在运行
        $isAlive = $pid && posix_kill($pid, 0);
        //停止命令
        if(isset(static::$arguments['stop']))
        {
            //如果进程非运行中
            if(!$isAlive)
            {
                static::log('进程并未运行');
            }
            //尝试向进程发送停止信号
            posix_kill($pid, SIGINT);
            $timeout = 5; //超时时间
            $start = time();
            while(1)
            {
                // 检查主进程是否存活
                $isAlive = posix_kill($pid, 0);
                if($isAlive)
                {
                    // 检查是否超过$timeout时间
                    if(time() - $start >= $timeout)
                    {
                        static::log('尝试停止进程失败');
                    }
                    usleep(10000);
                    continue;
                }
                //此处因为还没有 fork 所以不存在 $_pid 是可以在 log 中 exit 的
                static::log('停止进程执行成功');
                //所以下面的 exit 被省略了 依然能够退出
                exit(0);
            }
        }

        //平滑重启逻辑
        else if(isset(static::$arguments['reload']) && $isAlive)
        {
            //尝试向进程发送重启信号
            posix_kill($pid, SIGUSR1);
            //TODO 此处的sleep 本意是等待原始进程退出 正确清理 可以存在也可以不存在
            //TODO 探讨？此处不存在是存在问题：比如 原始业务退出时间过长
            //sleep(2);
        }
        else if($isAlive)
        {
            //进程已经存在 默认退出
            static::log('进程已经运行中 '.$pid);
        }
    }

    /**
     * 创建子进程之前检查
     */
    protected static function daemon()
    {
        //检查扩展
        if(!function_exists('posix_kill') || !function_exists('pcntl_fork'))
        {
            static::log('请重新编译PHP 加入 --enable-pcntl 去掉 --disable-posix');
        }

        //检查运行模式
        if(php_sapi_name() != 'cli')
        {
            static::log('当前脚本只能运行在 CLI 模式');
        }

        //尝试fork 子进程
        $_pid  = pcntl_fork ();
        if (static::$_pid  == - 1) {
            static::log ( '创建子进程失败');
        } else if ($_pid ) {
            //父进程 fork 成功 退出
            static::log('创建进程成功 '.$_pid .' 切换入后台模式');
            exit ( 0 );
        }

        //因为父进程已经在前面 exit
        //以下代码都是在fork 出来的子进程中进行执行 所有输出都将写入日志文件

        //进程尝试从当前终端脱离成为独立进程 -- 这个在 fork 之前和之后都可以调用
        if (posix_setsid () == - 1) {
            throw new Exception( '尝试从当前终端脱离失败');
        }

        //获取当前进程pid
        static::$_pid = posix_getpid();
        static::$_masterPid = static::$_pid;
        static::log('主进程启动成功');

        //写入pid 为何放置在这边 因为尝试用root 身份创建了 pid 以及 log 下文切换为非用户身份后会导致写入失败
        static::savePid();

        //尝试将进程切换入用户态 可以省略
        static::setProcessUser();
    }

    /**
     * 尝试设置运行当前进程的用户 from workerman
     *
     * 切换为指定用户态会存在问题 之前的 写入 到 /var/run /var/log 逻辑不通了。。。 还是尝试取消此逻辑？ TODO
     *
     * @return void
     */
    protected static function setProcessUser()
    {
        if(empty(static::$runAsUser) || posix_getuid() !== 0) // 0 run as root ?
        {
            return;
        }
        //获取指定用户名用户系统信息
        $userInfo = posix_getpwnam(static::$runAsUser);
        if($userInfo['uid'] != posix_getuid() || $userInfo['gid'] != posix_getgid())
        {
            //修改日志所属用户
            chown(static::getLogFile(),$userInfo['uid']);

            //尝试进入用户态
            if(!posix_setgid($userInfo['gid']) || !posix_setuid($userInfo['uid']))
            {
                static::log( '无法以用户 '.static::$runAsUser .' 的身份执行');
            }
        }
    }

    /**
     * 获取pid 文件名
     * @return string
     */
    protected static function getPidFile()
    {
        return static::$pidFile ? static::$pidFile : static::$pidDir.DIRECTORY_SEPARATOR.static::$uniqueName.'.pid';
    }

    /**
     * 保存 pid 到日志
     * @throws Exception
     */
    protected static function savePid()
    {
        @mkdir(static::$pidDir.DIRECTORY_SEPARATOR);
        if(false === @file_put_contents(static::getPidFile(), static::$_pid))
        {
            throw new \Exception('写入PID文件失败 请检查是否有文件写入权限 ' . static::getPidFile());
        }
    }

    /**
     * 获取日志文件名
     * @return string
     */
    protected static function getLogFile()
    {
        return static::$logFile ? static::$logFile : static::$logDir.DIRECTORY_SEPARATOR.static::$uniqueName.'.log';
    }


    /**
     * 改写错误输出
     * @throws Exception
     */
    protected static function resetStd()
    {
        global $STDOUT, $STDERR;
        $handle = fopen(static::$stdoutFile,"a");
        if($handle)
        {
            unset($handle);
            @fclose(STDOUT);
            @fclose(STDERR);
            $STDOUT = fopen(static::$stdoutFile,"a");
            $STDERR = fopen(static::$stdoutFile,"a");
        }
        else
        {
            throw new Exception('can not open stdoutFile ' . static::$stdoutFile);
        }
    }

    /**
     * 获取错误类型对应的意义 from workerman
     * @param integer $type
     * @return string
     */
    protected static function getErrorType($type)
    {
        switch($type)
        {
            case E_ERROR: // 1 //
                return 'E_ERROR';
            case E_WARNING: // 2 //
                return 'E_WARNING';
            case E_PARSE: // 4 //
                return 'E_PARSE';
            case E_NOTICE: // 8 //
                return 'E_NOTICE';
            case E_CORE_ERROR: // 16 //
                return 'E_CORE_ERROR';
            case E_CORE_WARNING: // 32 //
                return 'E_CORE_WARNING';
            case E_COMPILE_ERROR: // 64 //
                return 'E_COMPILE_ERROR';
            case E_COMPILE_WARNING: // 128 //
                return 'E_COMPILE_WARNING';
            case E_USER_ERROR: // 256 //
                return 'E_USER_ERROR';
            case E_USER_WARNING: // 512 //
                return 'E_USER_WARNING';
            case E_USER_NOTICE: // 1024 //
                return 'E_USER_NOTICE';
            case E_STRICT: // 2048 //
                return 'E_STRICT';
            case E_RECOVERABLE_ERROR: // 4096 //
                return 'E_RECOVERABLE_ERROR';
            case E_DEPRECATED: // 8192 //
                return 'E_DEPRECATED';
            case E_USER_DEPRECATED: // 16384 //
                return 'E_USER_DEPRECATED';
        }
        return "";
    }

    /**
     * 日志记录逻辑
     * @param $message
     * @param $exit
     */
    public static function log($message = '')
    {
        //非子进程内部 日志直接输出
        if(!static::$_pid)
        {
            echo $message.PHP_EOL;
            exit();
        }

        file_put_contents(static::getLogFile(),static::_debug().' ['.static::$_pid.'] '.$message.PHP_EOL,FILE_APPEND);
    }

    /**
     * 时间以及内存使用记录
     *
     * @return string
     */
    public static function _debug()
    {
        static $memory;

        list ($usec, $sec) = explode(" ", microtime());

        if(!static::$_startTime)
        {
            static::$_startTime = $usec + $sec;
            $memory = memory_get_usage(true)/1024 / 1024;
        }

        return date('Y-m-d H:i:s').'('.($usec + $sec - static::$_startTime).'--'.round(memory_get_usage(true) /1024 / 1024 - $memory).'MB)';
    }

    /**
     *
     * 任务脚本
     *
     * @param callable $job
     */
    public function job(callable $job)
    {
        static::$_job = $job;
    }

    /**
     * 根据索引获取子进程ID
     *
     * @param $index
     * @return int|mixed
     */
    static function getWorkerPid($index)
    {
        return isset(static::$_workers[$index]) ? static::$_workers[$index] : 0;
    }

    /**
     * 获取子进程索引
     *
     * @param $pid
     * @return int|string
     */
    static function getWorkerIndex($pid)
    {
        foreach(static::$_workers as $index => $_pid)
        {
            if($_pid === $pid){
                return $index;
            }
        }
    }

    /**
     * 创建子进程
     */
    public static function forkWorkers()
    {
        for($index = 1;$index <= static::$workerNum;$index++)
        {
            static::forkOneWorker($index);
        }
    }

    /**
     * 创建对应索引的子进程
     *
     * @param $index
     */
    public static function forkOneWorker($index)
    {
        $pid = static::getWorkerPid($index);
        if($pid)
        {
            return;
        }

        //尝试fork 子进程
        $pid  = pcntl_fork ();
        if ($pid  == - 1) {
            throw new Exception('创建子进程失败');
        } else if ($pid ) {
            //父进程记录创建的子进程信息
            static::$_workers[$index] = $pid;
        }
        else
        {
            //以下为子进程执行任务代码的逻辑
            //TODO 异常退出 while 循环导致的 register shutdown function 触发问题
            //worker 要处理的事情
            //记得清理一些从父集成来的变量
            //记录自己的pid
            static::$_pid = posix_getpid();
            static::$index = $index;
            static::$_workers = [];
            //主要用做清理一些父进程注册的信号 如果有特殊需求的话
            static::registerSignalHandlerChild();
            //默认循环 TODO 可以外层自带循环 系统提供循环处理能力
            while(1)
            {
                static::signalDispatch();
                call_user_func(static::$_job);
//                break;
                //TODO 如果此处直接 退出循环 系统会调用父进程注册的shutdown function 好奇怪
                //难道会跳过 exit ？？？
            }
//            die();
            exit(255);
        }
    }

    /**
     *
     * 监测
     *
     * TODO 文件变动 日志rotate 貌似得用libevent
     * TODO
     */
    public static function monitorWorkers()
    {
//        static::timer();
        while(1)
        {
            //这个重要 否则就无法退出这个循环了
            static::signalDispatch();

            $status = 0;
            //进入阻塞模式 会一直等待 下面的其他代码只有 捕获子进程退出之后才会执行
            //$pid = pcntl_wait($status,WUNTRACED);
            //非阻塞模式 可以做其他事情
            $pid = pcntl_wait($status,WNOHANG);
            if($pid > 0)
            {
                static::log('监测到子进程退出 '.$pid);
                $index = static::getWorkerIndex($pid);
                unset(static::$_workers[$index]);

                //当不是在停止的时候 启动新进程
                if(static::$_status !== static::STATUS_SHUTDOWN)
                {
                    static::log('尝试fork新进程');
                    static::forkOneWorker($index);
                }
            }

            //以下代码 pcntl wait nohang 才能执行的
            //尝试日志逻辑 rotate
            static::logRotate();
            sleep(1);
        }
    }

    /**
     * 日志定时清理
     */
    public static function logRotate()
    {
        if(floor((time() - static::$_logRotateTime) / static::$logRotate) >= 1)
        {
            static::$_logRotateTime = time();
            static::log('执行日志清理'.static::$_logRotateTime);

            $fp = fopen(static::getLogFile(),'r+');
            ftruncate($fp,0);
            fclose($fp);
        }
    }

    /**
     * 试验性质模块 本意是 pcntl_wait 和 event loop 如果能并发最好 目前看 无法并发
     *
     * 现采用 pcntl 非阻塞模式
     */
    public static function timer()
    {

        $base = new \EventBase();
        $timer = new \Event( $base, -1, \Event::TIMEOUT | \Event::PERSIST, function(){
            static::log('event loop '.time());
        } );
        $tick = 2;
        $timer->add(  $tick );
        //加入这个后 后续就都不执行了 TODO ！！！
        $base->loop();
    }

    /**
     * 判断子进程是否存活
     *
     * @param $pid
     * @return bool
     */
    public static function isChildAlive($pid)
    {
        $status = 0;
        $res = pcntl_waitpid($pid,$status,WNOHANG);
        //异常 或者退出
        if($res == -1 || $res > 0){
            return false;
        }
        return true;
    }

    /**
     * 尝试关闭所有子进程
     */
    protected static function stopAll()
    {
        static::$_status = self::STATUS_SHUTDOWN;
        //尝试发送中止信号
        foreach(static::$_workers as $index => $pid)
        {
            posix_kill($pid, SIGINT);
//            posix_kill($pid, SIGTERM);
        }

        //尝试查询子进程状态
        foreach(static::$_workers as $index => $pid)
        {
            $timeout = 5; //超时时间
            $start = time();
            while(1)
            {
                //检查子进程状态
                if(!static::isChildAlive($pid))
                {
                    unset(static::$_workers[$index]);
                    break;
                }
                // 检查是否超过$timeout时间
                if(time() - $start >= $timeout)
                {
                    static::log('关闭子进程失败 '.$pid);
                    break;
                }
                usleep(10000);
            }
        }

        if(empty(static::$_workers))
        {
            static::log('所有子进程退出完毕');
        }
        else
        {
            static::log('部分子进程退出失败 ['.implode(',',static::$_workers).']');
        }
    }

    /**
     * @important 每次业务循环结束或者开始都需要条用此代码来处理一些信号比如重启等
     */
    public static function signalDispatch()
    {
        pcntl_signal_dispatch();
    }

    /**
     * 子进程信号注册
     */
    protected static function registerSignalHandlerChild()
    {
        $signalHandler = '\Npc\Worker::signalHandlerChild';

        pcntl_signal(SIGINT, SIG_IGN, false);
        pcntl_signal(SIGTERM, SIG_IGN, false);
        pcntl_signal(SIGUSR1, SIG_IGN, false);
        pcntl_signal(SIGUSR2, SIG_IGN, false);
        pcntl_signal(SIGIO, SIG_IGN, false);

        //接管系统信号
        pcntl_signal(SIGTERM, $signalHandler, false);
        //不允许的行为
        //pcntl_signal(SIGKILL, [$this,'signalHandler'], false);
        //自定义的停止信号
        pcntl_signal(SIGINT, $signalHandler, false);
        //自定义的重启信号
        pcntl_signal(SIGUSR1, $signalHandler, false);
        //尚未用到
        pcntl_signal(SIGUSR2, $signalHandler, false);
        //尚未用到
        $reg = pcntl_signal(SIGPIPE, SIG_IGN, false);

        static::log('子进程注册信号处理 '.($reg ? '成功':'失败'));
    }

    /**
     * 主进程信号注册
     */
    protected function registerSignalHandlerMaster()
    {
        $signalHandler = '\Npc\Worker::signalHandlerMaster';

        //设置此处后 monitor 获取不到子进程退出信号 否则系统会通知并由主进程来维护子进程状态
//        pcntl_signal(SIGCHLD, SIG_IGN, false);

        //接管系统信号
        pcntl_signal(SIGTERM, $signalHandler, false);
        //不允许的行为
        //pcntl_signal(SIGKILL, [$this,'signalHandler'], false);
        //自定义的停止信号
        pcntl_signal(SIGINT, $signalHandler, false);
        //自定义的重启信号
        pcntl_signal(SIGUSR1, $signalHandler, false);
        //尚未用到
        pcntl_signal(SIGUSR2, $signalHandler, false);
        //尚未用到
        $reg = pcntl_signal(SIGPIPE, SIG_IGN, false);


        static::log('主进程注册信号处理 '.($reg ? '成功':'失败'));
    }

    /**
     * 主进程信号处理
     * @param $signal
     */
    protected static function signalHandlerMaster($signal)
    {
        static::log('主进程信号捕获 '.$signal);
        switch ($signal) {
            case SIGINT:
//            case SIGTERM:
                static::log('停止');
                static::stopAll();
                //执行pid 清理？ 测试发现 这个删除与否并不重要 反而不删除可能更好？
                //@unlink(static::getPidFile());
                exit(0);
                break;
            case SIGUSR1:
            case SIGUSR2:
                static::log('重启');
                exit(0);
                break;
            default:
                break;
        }
    }

    /**
     * 注册退出监控
     */
    public function registerShutdownHandler()
    {
        static::log('注册退出函数');
        register_shutdown_function(array($this,'shutdownHandler'));
    }

    /**
     * 此处覆盖父类的shutdownHandler
     *
     * 奇怪的是这个shutdownHandler 经过测试 当子进程意外退出的时候（即  forkAll 之后的那个循环里面 如果直接 exit 会触发这个Handler ）
     *
     * 如果是接受信号的exit 却不会触发
     *
     * 而且文档说 term 信号不会触发 试了 还是会触发
     */
    public static function shutdownHandler()
    {
        $errors = error_get_last();
        if($errors && ($errors['type'] === E_ERROR ||
                $errors['type'] === E_PARSE ||
                $errors['type'] === E_CORE_ERROR ||
                $errors['type'] === E_COMPILE_ERROR ||
                $errors['type'] === E_RECOVERABLE_ERROR ))
        {
            static::log(static::getErrorType($errors['type']) . " {$errors['message']} in {$errors['file']} on line {$errors['line']}");
        }

        static::log('shutdownHandler 退出');
    }

    protected static function signalHandlerChild($signal)
    {
        static::log('子进程捕获信号 '.$signal);
        switch ($signal) {
            case SIGINT:
//            case SIGTERM:
                static::log('停止');
                //TODO 奇怪 这里为什么没有调用 shutdownHandler？
                exit(0);
                break;
            case SIGUSR1:
            case SIGUSR2:
                static::log('重启');
                exit(0);
                break;
            default:
                break;
        }
    }

}