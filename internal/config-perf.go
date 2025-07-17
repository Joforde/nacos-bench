package internal

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"golang.org/x/time/rate"
)

var configClients []config_client.IConfigClient
var dataIds []string

func InitConfig(perfConfig PerfConfig) {
	fmt.Println("开始初始化配置客户端...")
	clientConfig := constant.ClientConfig{
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		LogDir:              "/data/nacos/log",
		CacheDir:            "/data/nacos/cache",
		LogLevel:            "info",
		// 关闭本地文件缓存
		DisableUseSnapShot:   true,  // 禁用快照
		UpdateCacheWhenEmpty: false, // 空配置时不更新缓存
		UpdateThreadNum:      0,     // 禁用更新线程
	}

	serverConfigs := make([]constant.ServerConfig, 0)
	for _, addr := range strings.Split(perfConfig.NacosAddr, ",") {
		serverConfigs = append(serverConfigs, constant.ServerConfig{
			IpAddr:      addr,
			ContextPath: "/nacos",
			Port:        8848,
		})
		fmt.Printf("添加服务器配置: %s:8848\n", addr)
	}

	// 多线程创建配置客户端
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < perfConfig.ClientCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer mu.Unlock()
			client, err := clients.NewConfigClient(vo.NacosClientParam{
				ClientConfig:  &clientConfig,
				ServerConfigs: serverConfigs,
			})
			if err != nil {
				fmt.Printf("创建配置客户端失败: %v\n", err)
				return
			}
			mu.Lock()
			configClients = append(configClients, client)
		}()
	}
	wg.Wait()
	fmt.Printf("成功创建 %d 个配置客户端\n", len(configClients))

	if len(configClients) == 0 {
		fmt.Println("错误: 没有成功创建任何配置客户端")
		return
	}

	// 检查是否需要初始化配置
	configClient1 := configClients[0]
	fmt.Println("检查配置是否存在...")
	content, err := configClient1.GetConfig(vo.ConfigParam{
		DataId: "nacos.config.perf.test.dataId." + strconv.Itoa(perfConfig.ConfigCount-1),
		Group:  "DEFAULT_GROUP",
	})

	needInitConfig := true
	if err == nil && content != "" {
		fmt.Println("配置已存在，跳过初始化")
		needInitConfig = false
	} else if err != nil {
		fmt.Printf("检查配置时出错: %v\n", err)
	} else {
		fmt.Println("配置不存在，将进行初始化")
	}

	// 生成所有 dataId
	for i := 0; i < perfConfig.ConfigCount; i++ {
		dataId := "nacos.config.perf.test.dataId." + strconv.Itoa(i)
		dataIds = append(dataIds, dataId)
	}

	if needInitConfig {
		fmt.Printf("开始多线程批量创建 %d 个配置项...\n", perfConfig.ConfigCount)
		startTime := time.Now()

		// 计算每个线程处理的配置数量
		workerCount := 10 // 使用10个线程
		if workerCount > perfConfig.ConfigCount {
			workerCount = perfConfig.ConfigCount
		}
		configsPerWorker := perfConfig.ConfigCount / workerCount
		remainingConfigs := perfConfig.ConfigCount % workerCount

		var configWg sync.WaitGroup
		var successCount int32
		var failCount int32

		// 启动多个线程批量创建配置
		for workerID := 0; workerID < workerCount; workerID++ {
			configWg.Add(1)
			go func(workerID int) {
				defer configWg.Done()

				// 计算当前线程需要处理的配置范围
				startIdx := workerID * configsPerWorker
				endIdx := startIdx + configsPerWorker
				if workerID == workerCount-1 {
					endIdx += remainingConfigs // 最后一个线程处理剩余的配置
				}

				// 为每个线程分配一个客户端
				client := configClients[workerID%len(configClients)]

				// 批量创建配置
				for i := startIdx; i < endIdx; i++ {
					dataId := dataIds[i]

					// 检查配置是否已存在
					res, err := client.GetConfig(vo.ConfigParam{
						DataId: dataId,
						Group:  "DEFAULT_GROUP",
					})
					if err == nil && res != "" {
						continue // 配置已存在，跳过
					}

					// 创建配置
					success, err := client.PublishConfig(vo.ConfigParam{
						DataId:  dataId,
						Group:   "DEFAULT_GROUP",
						Content: generateRandomString(perfConfig.ConfigContentLength),
					})

					if err != nil {
						newFailCount := atomic.AddInt32(&failCount, 1)
						if newFailCount <= 5 { // 只显示前5个错误
							fmt.Printf("发布配置 %s 失败: %v\n", dataId, err)
						}
					} else if success {
						atomic.AddInt32(&successCount, 1)
					} else {
						newFailCount := atomic.AddInt32(&failCount, 1)
						if newFailCount <= 5 { // 只显示前5个错误
							fmt.Printf("发布配置 %s 失败: 返回false\n", dataId)
						}
					}
				}
			}(workerID)
		}

		configWg.Wait()
		duration := time.Since(startTime)

		fmt.Printf("配置创建完成，耗时: %v\n", duration)
		fmt.Printf("成功创建: %d 个配置\n", successCount)
		fmt.Printf("创建失败: %d 个配置\n", failCount)
		fmt.Printf("平均创建速度: %.2f 配置/秒\n", float64(successCount)/duration.Seconds())
	}

	fmt.Printf("初始化完成，共准备 %d 个配置项\n", len(dataIds))

	if perfConfig.PerfApi == "configSubscribe" {
		for _, client := range configClients {
			dataId := dataIds[0]
			client.ListenConfig(vo.ConfigParam{
				DataId: dataId,
				Group:  "DEFAULT_GROUP",
				OnChange: func(namespace, group, dataId, data string) {
				},
			})
		}
	}
}

func publicConfig(client config_client.IConfigClient, limiter *rate.Limiter, dataId string, dataLength int) {
	if err := limiter.Wait(context.Background()); err != nil {
		return
	}
	client.PublishConfig(vo.ConfigParam{
		DataId:  dataId,
		Group:   "DEFAULT_GROUP",
		Content: generateRandomString(dataLength),
	})
}

func getConfig(client config_client.IConfigClient, limiter *rate.Limiter, dataId string) {
	if err := limiter.Wait(context.Background()); err != nil {
		return
	}
	client.GetConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  "DEFAULT_GROUP",
	})
}
func RunConfigPerf(perfConfig PerfConfig) {
	startTime := time.Now().UnixMilli()
	configCount := perfConfig.ConfigCount
	var wg sync.WaitGroup

	switch perfConfig.PerfApi {
	case "configPub":
		pubLimiter := rate.NewLimiter(rate.Limit(perfConfig.ConfigPubTps), 1)
		for i := 0; i < perfConfig.ClientCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					if perfConfig.PerfTimeSec > 0 && time.Now().UnixMilli()-startTime > int64(perfConfig.PerfTimeSec)*1000 {
						fmt.Printf("配置发布测试完成，耗时: %d 秒\n", perfConfig.PerfTimeSec)
						return
					}
					publicConfig(configClients[i], pubLimiter, dataIds[rand.Intn(configCount)], perfConfig.ConfigContentLength)
				}
			}()
		}
	case "configGet":
		getConfigLimiter := rate.NewLimiter(rate.Limit(perfConfig.ConfigGetTps), 1)
		for i := 0; i < perfConfig.ClientCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					if perfConfig.PerfTimeSec > 0 && time.Now().UnixMilli()-startTime > int64(perfConfig.PerfTimeSec)*1000 {
						fmt.Printf("配置查询测试完成，耗时: %d 秒\n", perfConfig.PerfTimeSec)
						return
					}
					getConfig(configClients[i], getConfigLimiter, dataIds[rand.Intn(configCount)])
				}
			}()
		}
	case "configSubscribe":
		//pubLimiter := rate.NewLimiter(rate.Limit(perfConfig.ConfigPubTps), 1)
		//for i := 0; i < perfConfig.ClientCount; i++ {
		//	go func() {
		//		for {
		//			if perfConfig.PerfTimeSec > 0 && time.Now().UnixMilli()-startTime > int64(perfConfig.PerfTimeSec)*1000 {
		//				return
		//			}
		//			publicConfig(configClients[i], pubLimiter, dataIds[rand.Intn(configCount)], perfConfig.ConfigContentLength)
		//		}
		//	}()
		//}
	default:
		panic("unknown perf api")
	}

	// 等待所有 goroutine 完成
	wg.Wait()
	fmt.Println("所有测试 goroutine 已完成")
}
