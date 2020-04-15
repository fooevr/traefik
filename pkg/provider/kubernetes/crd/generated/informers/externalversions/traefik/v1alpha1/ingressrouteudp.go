/*
The MIT License (MIT)

Copyright (c) 2016-2020 Containous SAS

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

// Code generated by informer-gen. DO NOT EDIT.

package v1alpha1

import (
	time "time"

	versioned "github.com/containous/traefik/v2/pkg/provider/kubernetes/crd/generated/clientset/versioned"
	internalinterfaces "github.com/containous/traefik/v2/pkg/provider/kubernetes/crd/generated/informers/externalversions/internalinterfaces"
	v1alpha1 "github.com/containous/traefik/v2/pkg/provider/kubernetes/crd/generated/listers/traefik/v1alpha1"
	traefikv1alpha1 "github.com/containous/traefik/v2/pkg/provider/kubernetes/crd/traefik/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
)

// IngressRouteUDPInformer provides access to a shared informer and lister for
// IngressRouteUDPs.
type IngressRouteUDPInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1alpha1.IngressRouteUDPLister
}

type ingressRouteUDPInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewIngressRouteUDPInformer constructs a new informer for IngressRouteUDP type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewIngressRouteUDPInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredIngressRouteUDPInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredIngressRouteUDPInformer constructs a new informer for IngressRouteUDP type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredIngressRouteUDPInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.TraefikV1alpha1().IngressRouteUDPs(namespace).List(options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.TraefikV1alpha1().IngressRouteUDPs(namespace).Watch(options)
			},
		},
		&traefikv1alpha1.IngressRouteUDP{},
		resyncPeriod,
		indexers,
	)
}

func (f *ingressRouteUDPInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredIngressRouteUDPInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *ingressRouteUDPInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&traefikv1alpha1.IngressRouteUDP{}, f.defaultInformer)
}

func (f *ingressRouteUDPInformer) Lister() v1alpha1.IngressRouteUDPLister {
	return v1alpha1.NewIngressRouteUDPLister(f.Informer().GetIndexer())
}
