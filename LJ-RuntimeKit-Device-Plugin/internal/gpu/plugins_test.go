/*
Copyright (c) 2024-2026 Alibaba Cloud.

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

package gpu

import (
	"errors"
	"testing"

	xpuplugin "github.com/lj-runtimekit/device-plugin/internal/plugin"

	"github.com/stretchr/testify/assert"
)

// fakeStartablePlugin records what a startup round did to it.
type fakeStartablePlugin struct {
	name     string
	startErr error
	started  bool
	stopped  bool
}

func (p *fakeStartablePlugin) Name() string { return p.name }

func (p *fakeStartablePlugin) Start() error {
	if p.startErr != nil {
		return p.startErr
	}

	p.started = true
	return nil
}

func (p *fakeStartablePlugin) Stop() error {
	p.stopped = true
	return nil
}

func (p *fakeStartablePlugin) Unhealthy(*xpuplugin.DeviceBase) {}

// A round whose start fails must leave nothing running: the plugins that
// already started are stopped again. The caller registers a set only on
// success, so a survivor would be referenced nowhere, and the next round
// removes the same sockets, turning it into a server nobody can stop.
func TestStartPluginSetRollsBackOnFailure(t *testing.T) {
	first := &fakeStartablePlugin{name: "first"}
	second := &fakeStartablePlugin{name: "second", startErr: errors.New("register with kubelet failed")}
	third := &fakeStartablePlugin{name: "third"}

	started, ok := startPluginSet([]xpuplugin.PluginInterface{first, second, third})

	assert.False(t, ok)
	assert.Equal(t, 1, started)
	assert.True(t, first.started)
	assert.True(t, first.stopped, "a plugin started before the failure must be rolled back")
	assert.False(t, second.started)
	assert.False(t, second.stopped, "the failing plugin never started, it must not be stopped")
	assert.False(t, third.started, "a failure aborts the rest of the set")
	assert.False(t, third.stopped)
}

func TestStartPluginSetStartsEveryPlugin(t *testing.T) {
	first := &fakeStartablePlugin{name: "first"}
	second := &fakeStartablePlugin{name: "second"}

	started, ok := startPluginSet([]xpuplugin.PluginInterface{first, second})

	assert.True(t, ok)
	assert.Equal(t, 2, started)
	assert.True(t, first.started)
	assert.True(t, second.started)
	assert.False(t, first.stopped)
	assert.False(t, second.stopped)
}

func TestStartPluginSetHandlesAnEmptySet(t *testing.T) {
	started, ok := startPluginSet(nil)

	assert.True(t, ok)
	assert.Equal(t, 0, started)
}

// A topology sync that runs while the restart loop replaces the plugin set must
// not start an rdma plugin of its own: the new set is about to be installed and
// nothing would own or stop that instance.
func TestEnsureRDMAStartedSkipsDuringRebuild(t *testing.T) {
	dm := &DeviceManager{}

	dm.beginRebuild()
	assert.ErrorContains(t, dm.ensureRDMAStarted(), "being rebuilt")

	dm.endRebuild()
}

// Health reports iterate the set of a run while the restart loop replaces it, so
// they must work on a copy instead of the shared slice.
func TestSnapshotPluginsCopiesTheSet(t *testing.T) {
	dm := &DeviceManager{}
	dm.setPlugins([]xpuplugin.PluginInterface{&fakeRdmaPlugin{}})

	plugins := dm.snapshotPlugins()
	assert.Len(t, plugins, 1)

	dm.setPlugins(nil)
	assert.Len(t, plugins, 1)
	assert.Empty(t, dm.snapshotPlugins())
}
