# k8s-self-healing-stack
capstone project
# Kubernetes Automated Health Checker & Self-Healing Engine

An automated operations system designed to monitor Kubernetes cluster health, parse core runtime metrics, detect common container failures (`CrashLoopBackOff`, `ErrImagePull`), execute rapid self-healing pod evictions, and expose cluster status via a built-in web dashboard ledger and Slack alerting hooks.

## 🏗️ Project Architecture
```text
                  +-----------------------------------------+

                  |               AWS EC2 Node              |
                  |  +-----------------------------------+  |
                  |  |            K3s Cluster            |  |
                  |  |                                   |  |
                  |  |  +-----------------------------+  |  |
                  |  |  |  Self-Healing Pod (Go)      |  |  |
                  |  |  |  -------------------------  |  |  |
                  |  |  |  • Informer / Control Loop  |  |  |
                  |  |  |  • Web Dashboard Engine     |====> Web Browser (Port 30080)
                  |  |  +--------------||-------------+  |  |
                  |  |                 ||                 |  |
                  |  |  +--------------\/-------------+  |  |
                  |  |  |   Target Pod Dependencies   |  |  |
                  |  |  +-----------------------------+  |  |
                  |  +-----------------------------------+  |
                  +-----------------------------------------+
```

---

## 📂 Directory Structure

```text
k8s-self-healing-stack/
├── .github/
│   └── workflows/
│       └── deploy.yml          # GitHub Actions CI/CD automation pipeline
├── cmd/
│   ├── healer/
│   │   └── main.go             # Entry point of the Go Self-Healing Agent & Dashboard
│   └── templates/
│       └── index.html          # Web dashboard Frontend UI HTML template
├── internal/
│   └── controller/
│       └── pod_controller.go   # Core monitoring and remediation control loop
├── deployments/
│   ├── k8s/
│   │   ├── self-healing-agent.yaml   # RBAC settings, config maps, & agent deployments
│   │   └── test-failure-pod.yaml     # Intentional crash deployment for validation
│   └── monitoring/
│       ├── alertmanager-config.yaml  # Helm value overrides for Slack notifications
│       └── prometheus-alerts.yaml    # Custom Prometheus alerting rule definitions
├── scripts/
│   ├── install-k3s.sh          # Environment bootstrap script for EC2
│   └── setup-monitoring.sh     # Automation script to install Prometheus stack
├── Dockerfile                  # Multi-stage production container build instructions
├── go.mod                      # Go module dependencies manifest
├── go.sum                      # Go module dependencies verification hashes
└── README.md                   # Project setup and usage documentation
```

---

## 💻 Source Code & Implementation

### 1. Main Entrypoint & Web Engine (`cmd/healer/main.go`)
This file combines the Kubernetes control loop with a lightweight Go `net/http` web server to serve real-time self-healing states.

```go
package main

import (
	"context"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

type LogEntry struct {
	Timestamp string
	PodName   string
	Namespace string
	Reason    string
	Status    string
}

var (
	historyLog []LogEntry
	logMutex   sync.Mutex
	startTime  = time.Now()
)

type Controller struct {
	clientset kubernetes.Interface
	informer  cache.SharedIndexInformer
	queue     workqueue.RateLimitingInterface
}

func NewController(client kubernetes.Interface) *Controller {
	listWatcher := cache.NewListWatchFromClient(
		client.CoreV1().RESTClient(),
		"pods",
		metav1.NamespaceAll,
		fields.Everything(),
	)

	queue := workqueue.NewRateLimitingWorkQueue(workqueue.DefaultControllerRateLimiter())
	informer := cache.NewSharedIndexInformer(listWatcher, &corev1.Pod{}, 0, cache.Indexers{})

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil { queue.Add(key) }
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			if err == nil { queue.Add(key) }
		},
	})

	return &Controller{
		clientset: client,
		informer:  informer,
		queue:     queue,
	}
}

func (c *Controller) Run(stopCh <-chan struct{}) {
	defer c.queue.ShutDown()
	go c.informer.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		log.Fatal("Failed syncing cache states")
	}

	log.Println("Self-healing worker loop active...")
	for c.processNextItem() {}
}

func (c *Controller) processNextItem() bool {
	obj, quit := c.queue.Get()
	if quit { return false }
	defer c.queue.Done(obj)

	key := obj.(string)
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil { return true }

	pod, err := c.clientset.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil { return true }

	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Waiting != nil {
			reason := status.State.Waiting.Reason
			if reason == "CrashLoopBackOff" || reason == "ErrImagePull" {
				logMutex.Lock()
				if len(historyLog) == 0 || historyLog[len(historyLog)-1].PodName != name {
					historyLog = append(historyLog, LogEntry{
						Timestamp: time.Now().Format("2006-01-02 15:04:05"),
						PodName:   name,
						Namespace: namespace,
						Reason:    reason,
						Status:    "Remediating (Eviction Issued)",
					})
				}
				logMutex.Unlock()

				log.Printf("[ALERT] Unhealthy pod detected: %s/%s. Reason: %s. Healing active...", namespace, name, reason)
				
				err := c.clientset.CoreV1().Pods(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
				logMutex.Lock()
				if err != nil {
					log.Printf("Failed evicting pod %s: %v", name, err)
					historyLog[len(historyLog)-1].Status = "Failed to Evict"
				} else {
					log.Printf("[SUCCESS] Evicted pod %s/%s for self-healing resolution.", namespace, name)
					historyLog[len(historyLog)-1].Status = "Successfully Evicted"
				}
				logMutex.Unlock()
			}
		}
	}
	return true
}

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	tmplData := struct {
		Uptime string
		Logs   []LogEntry
	}{
		Uptime: time.Since(startTime).Round(time.Second).String(),
		Logs:   historyLog,
	}

	tmpl, err := template.ParseFiles("cmd/templates/index.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("Template rendering error: %v", err), http.StatusInternalServerError)
		return
	}
	tmpl.Execute(w, tmplData)
}

func main() {
	var config *rest.Config
	var err error

	config, err = rest.InClusterConfig()
	if err != nil {
		kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil { log.Fatalf("Kubeconfig error: %v", err) }
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil { log.Fatalf("Failed creating API client: %v", err) }

	stopCh := make(chan struct{})
	defer close(stopCh)

	controller := NewController(clientset)
	go controller.Run(stopCh)

	http.HandleFunc("/", handleDashboard)
	log.Println("Starting Web Dashboard Interface on port :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

### 2. Dashboard Frontend Template (`cmd/templates/index.html`)
```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>K8s Self-Healing Dashboard</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f4f6f9; color: #333; margin: 0; padding: 20px; }
        .container { max-width: 1000px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }
        h1 { color: #1e293b; margin-top: 0; border-bottom: 2px solid #e2e8f0; padding-bottom: 10px; }
        .meta-box { background: #eff6ff; color: #1e40af; padding: 15px; border-radius: 6px; font-weight: bold; margin-bottom: 20px; display: inline-block; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th, td { padding: 12px 15px; text-align: left; border-bottom: 1px solid #e2e8f0; }
        th { background-color: #f8fafc; color: #64748b; font-weight: 600; text-transform: uppercase; font-size: 12px; }
        tr:hover { background-color: #f8fafc; }
        .status-success { background: #dcfce7; color: #15803d; padding: 4px 8px; border-radius: 4px; font-size: 13px; font-weight: 500; }
        .status-fail { background: #fee2e2; color: #b91c1c; padding: 4px 8px; border-radius: 4px; font-size: 13px; font-weight: 500; }
    </style>
    <meta http-equiv="refresh" content="5">
</head>
<body>
    <div class="container">
        <h1>Kubernetes Core Self-Healing Engine</h1>
        <div class="meta-box">⏱️ Engine System Uptime: {{.Uptime}}</div>
        
        <h2>Execution logs & Remediation Ledger</h2>
        <table>
            <thead>
                <tr>
                    <th>Timestamp</th>
                    <th>Pod Name</th>
                    <th>Namespace</th>
                    <th>Intercepted Failure</th>
                    <th>Remediation Status</th>
                </tr>
            </thead>
            <tbody>
                {{range .Logs}}
                <tr>
                    <td>{{.Timestamp}}</td>
                    <td><strong>{{.PodName}}</strong></td>
