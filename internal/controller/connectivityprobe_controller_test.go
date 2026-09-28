/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	monitoringv1alpha1 "github.com/idanherman/connectivity-probe-operator/api/v1alpha1"
)

var _ = Describe("ConnectivityProbe Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		connectivityprobe := &monitoringv1alpha1.ConnectivityProbe{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind ConnectivityProbe")
			err := k8sClient.Get(ctx, typeNamespacedName, connectivityprobe)
			if err != nil && errors.IsNotFound(err) {
				resource := &monitoringv1alpha1.ConnectivityProbe{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: monitoringv1alpha1.ConnectivityProbeSpec{
						Image: "example.com/connectivity-probe-agent:test",
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &monitoringv1alpha1.ConnectivityProbe{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance ConnectivityProbe")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &ConnectivityProbeReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			ds := &appsv1.DaemonSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-mesh-agent", Namespace: "default",
			}, ds)).To(Succeed())
			Expect(ds.Spec.Template.Spec.Containers[0].Image).To(Equal("example.com/connectivity-probe-agent:test"))
			var buckets string
			for _, env := range ds.Spec.Template.Spec.Containers[0].Env {
				if env.Name == "LATENCY_BUCKETS" {
					buckets = env.Value
				}
			}
			Expect(buckets).To(ContainSubstring("0.0005"))

			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-mesh-headless", Namespace: "default",
			}, svc)).To(Succeed())
			Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))

			sa := &corev1.ServiceAccount{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-agent", Namespace: "default",
			}, sa)).To(Succeed())

			updated := &monitoringv1alpha1.ConnectivityProbe{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			Expect(updated.Status.Conditions).NotTo(BeEmpty())
		})
	})
})
