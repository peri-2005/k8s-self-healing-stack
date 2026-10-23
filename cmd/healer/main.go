package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
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
			if err == nil {
				queue.Add(key)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			if err == nil {
				queue.Add(key)
			}
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

	log.Println("Self-healing worker loop running successfully...")
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
			// Intercept common broken application states
			if reason == "CrashLoopBackOff" || reason == "ErrImagePull" {
				log.Printf("[ALERT] Unhealthy pod detected: %s/%s. Reason: %s. Healing active...", namespace, name, reason)
				
				// Self-healing step: Evict the broken pod so the cluster controller provisioner rebuilds it
				err := c.clientset.CoreV1().Pods(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
				if err != nil {
					log.Printf("Failed evicting pod %s: %v", name, err)
				} else {
					log.Printf("[SUCCESS] Successfully evicted pod %s/%s for self-healing resolution.", namespace, name)
				}
			}
		}
	}
	return true
}

func main() {
	var config *rest.Config
	var err error

	// Automatically distinguish between running inside the cluster vs. local machine execution
	config, err = rest.InClusterConfig()
	if err != nil {
		kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			log.Fatalf("Kubeconfig parsing error: %v", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed creating API client: %v", err)
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

	controller := NewController(clientset)
	log.Println("Starting automated K8s Health Check and Self-Healing Engine...")
	controller.Run(stopCh)
}
