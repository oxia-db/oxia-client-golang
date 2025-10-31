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

package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientPool_GetActualAddress(t *testing.T) {
	pool := NewClientPool(nil, nil)
	poolInstance := pool.(*clientPool)

	address := poolInstance.getActualAddress("tls://xxxxaa:6648")
	assert.Equal(t, "xxxxaa:6648", address)

	actualAddress := poolInstance.getActualAddress("xxxxaaa:6649")
	assert.Equal(t, "xxxxaaa:6649", actualAddress)
}

func TestClientPool_GetTransportCredential(t *testing.T) {
	pool := NewClientPool(nil, nil)
	poolInstance := pool.(*clientPool)

	credential := poolInstance.getTransportCredential("tls://xxxxaa:6648")
	assert.Equal(t, "tls", credential.Info().SecurityProtocol)

	credential = poolInstance.getTransportCredential("xxxxaaa:6649")
	assert.Equal(t, "insecure", credential.Info().SecurityProtocol)
}
