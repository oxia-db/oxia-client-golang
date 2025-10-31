// Copyright 2023-2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package oxia

import (
	"context"
	"crypto/tls"
	"errors"

	rpc "github.com/oxia-db/oxia-client-golang/internal/provider"
	"github.com/oxia-db/oxia-client-golang/pkg/auth"
)

var _ AdminClient = (*adminClientImpl)(nil)

type adminClientImpl struct {
	adminAddr string

	clientPool rpc.ClientPool
}

func (admin *adminClientImpl) Close() error {
	return admin.clientPool.Close()
}

func (admin *adminClientImpl) ListNamespaces() *ListNamespacesResult {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return &ListNamespacesResult{
			Error: err,
		}
	}
	if client == nil {
		return &ListNamespacesResult{
			Error: errors.New("no coordinator admin client available"),
		}
	}

	namespaces, err := client.ListNamespaces(context.Background(), &proto.ListNamespacesRequest{})
	if err != nil {
		return &ListNamespacesResult{
			Error: err,
		}
	}
	return &ListNamespacesResult{
		Namespaces: namespaces.Namespaces,
	}
}

func NewAdminClient(adminAddr string, tlsConf *tls.Config, authentication auth.Authentication) (AdminClient, error) {
	c := rpc.NewClientPool(tlsConf, authentication)
	return &adminClientImpl{
		adminAddr:  adminAddr,
		clientPool: c,
	}, nil
}
