package runtime

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/sealerio/sealer/common"
	"github.com/sealerio/sealer/logger"
	v2 "github.com/sealerio/sealer/types/api/v2"
	"github.com/sealerio/sealer/utils"
	"github.com/sealerio/sealer/utils/exec"
	"github.com/sealerio/sealer/utils/net"
	"github.com/sealerio/sealer/utils/platform"
	"github.com/sealerio/sealer/utils/ssh"
	"golang.org/x/sync/errgroup"
)

func newK3sRuntime(cluster *v2.Cluster, clusterFileKubeConfig *KubeadmConfig) (Interface, error) {
	k := &K3sRuntime{
		Cluster: cluster,
		Config: &Config{
			ClusterFileKubeConfig: clusterFileKubeConfig,
			APIServerDomain:       DefaultAPIserverDomain,
		},
	}
	k.Config.RegConfig = GetRegistryConfig(k.getImageMountDir(), k.GetMaster0IP())
	return k, nil
}

func (k *K3sRuntime) Upgrade() error {
	return nil
}

func (k *K3sRuntime) Reset() error {
	logger.Info("Start to delete cluster: master %s, node %s", k.Cluster.GetMasterIPList(), k.Cluster.GetNodeIPList())
	if err := k.confirmDeleteNodes(); err != nil {
		return err
	}
	return k.reset()
}

func (k *K3sRuntime) JoinMasters(newMastersIPList []string) error {
	if len(newMastersIPList) != 0 {
		logger.Info("%s will be added as master", newMastersIPList)
	}
	return nil
}

func (k *K3sRuntime) JoinNodes(newNodesIPList []string) error {
	if len(newNodesIPList) != 0 {
		logger.Info("%s will be added as worker", newNodesIPList)
	}
	return nil
}

func (k *K3sRuntime) DeleteMasters(mastersIPList []string) error {
	if len(mastersIPList) != 0 {
		logger.Info("master %s will be deleted", mastersIPList)
		if err := k.confirmDeleteNodes(); err != nil {
			return err
		}
	}
	return nil
}

func (k *K3sRuntime) DeleteNodes(nodesIPList []string) error {
	if len(nodesIPList) != 0 {
		logger.Info("worker %s will be deleted", nodesIPList)
		if err := k.confirmDeleteNodes(); err != nil {
			return err
		}
	}
	return nil
}

func (k *K3sRuntime) confirmDeleteNodes() error {
	if !ForceDelete {
		if pass, err := utils.ConfirmOperation("Are you sure to delete these nodes? "); err != nil {
			return err
		} else if !pass {
			return fmt.Errorf("exit the operation of delete these nodes")
		}
	}
	return nil
}

func (k *K3sRuntime) GetClusterMetadata() (*Metadata, error) {
	return LoadMetadata(k.getRootfs())
}

func (k *K3sRuntime) UpdateCert(certs []string) error {
	return nil
}

type K3sRuntime struct {
	*sync.Mutex
	*v2.Cluster
	*Config
}

func (k *K3sRuntime) Init(cluster *v2.Cluster) error {
	return k.init(cluster)
}

func (k *K3sRuntime) getHostSSHClient(hostIP string) (ssh.Interface, error) {
	return ssh.NewStdoutSSHClient(hostIP, k.Cluster)
}

func (k *K3sRuntime) getRegistryHost() (host string) {
	ip, _ := net.GetSSHHostIPAndPort(k.RegConfig.IP)
	return fmt.Sprintf("%s %s", ip, k.RegConfig.Domain)
}

func (k *K3sRuntime) getImageMountDir() string {
	return platform.DefaultMountCloudImageDir(k.getClusterName())
}

func (k *K3sRuntime) getClusterName() string {
	return k.Cluster.Name
}

func (k *K3sRuntime) GerLoginCommand() string {
	return fmt.Sprintf("%s && %s",
		fmt.Sprintf(DockerLoginCommand, k.RegConfig.Username, k.RegConfig.Password, k.RegConfig.Domain+":"+k.RegConfig.Port),
		fmt.Sprintf(DockerLoginCommand, k.RegConfig.Username, k.RegConfig.Password, SeaHub+":"+k.RegConfig.Port))
}

func (k *K3sRuntime) reset() error {
	//k.resetNodes(k.GetNodeIPList())
	k.resetMasters(k.GetMasterIPList())
	//if the executing machine is not in the cluster
	if _, err := exec.RunSimpleCmd(fmt.Sprintf(RemoteRemoveAPIServerEtcHost, k.getAPIServerDomain())); err != nil {
		return err
	}
	/*for _, node := range k.GetNodeIPList() {
		err := k.deleteVIPRouteIfExist(node)
		if err != nil {
			return fmt.Errorf("failed to delete %s route: %v", node, err)
		}
	}*/
	k.DeleteRegistry()
	return nil
}

func (k *K3sRuntime) getAPIServerDomain() string {
	return k.Config.APIServerDomain
}

func (k *K3sRuntime) DeleteRegistry() error {
	ssh, err := k.getHostSSHClient(k.RegConfig.IP)
	if err != nil {
		return fmt.Errorf("failed to delete registry: %v", err)
	}

	cmd := fmt.Sprintf("docker rm -f %s", RegistryName)
	return ssh.CmdAsync(k.RegConfig.IP, cmd)
}

func (k *K3sRuntime) resetMasters(nodes []string) {
	for _, node := range nodes {
		if err := k.resetNode(node); err != nil {
			logger.Error("delete master %s failed %v", node, err)
		}
	}
}

func (k *K3sRuntime) resetNode(node string) error {
	ssh, err := k.getHostSSHClient(node)
	if err != nil {
		return fmt.Errorf("reset node failed %v", err)
	}
	uninstallCmd := "sh /usr/local/bin/k3s-uninstall.sh"
	if err := ssh.CmdAsync(node, uninstallCmd,
		RemoveKubeConfig,
		fmt.Sprintf(RemoteRemoveAPIServerEtcHost, k.getAPIServerDomain()),
		fmt.Sprintf(RemoteRemoveAPIServerEtcHost, SeaHub),
		fmt.Sprintf(RemoteRemoveAPIServerEtcHost, k.RegConfig.Domain),
		fmt.Sprintf(RemoteRemoveRegistryCerts, k.RegConfig.Domain),
		fmt.Sprintf(RemoteRemoveRegistryCerts, SeaHub)); err != nil {
		return err
	}
	return nil
}

func (k *K3sRuntime) getRootfs() string {
	return common.DefaultTheClusterRootfsDir(k.getClusterName())
}

func (k *K3sRuntime) init(cluster *v2.Cluster) error {
	logger.Info("->>>>>>>>>>>> running.........")
	pipeline := []func() error{
		k.GenerateCert,
		k.ApplyRegistry,
		k.InitMaster0,
	}
	for _, f := range pipeline {
		if err := f(); err != nil {
			return fmt.Errorf("failed to init master0 %v", err)
		}
	}
	return nil
}

func (k *K3sRuntime) GenerateCert() error {
	/*hostName, err := k.getRemoteHostName(k.GetMaster0IP())
	if err != nil {
		return err
	}
	err = cert.GenerateCert(
		k.getPKIPath(),
		k.getEtcdCertPath(),
		k.getCertSANS(),
		k.GetMaster0IP(),
		hostName,
		k.getSvcCIDR(),
		k.getDNSDomain(),
	)
	if err != nil {
		return fmt.Errorf("generate certs failed %v", err)
	}
	err = k.sendNewCertAndKey(k.GetMasterIPList()[:1])
	if err != nil {
		return err
	}*/
	err := k.GenerateRegistryCert()
	if err != nil {
		return err
	}
	return k.SendRegistryCert(k.GetMasterIPList()[:1])
}

func (k *K3sRuntime) GenerateRegistryCert() error {
	return GenerateRegistryCert(k.getCertsDir(), k.RegConfig.Domain)
}

func (k *K3sRuntime) SendRegistryCert(host []string) error {
	err := k.sendRegistryCertAndKey()
	if err != nil {
		return err
	}
	return k.sendRegistryCert(host)
}

func (k *K3sRuntime) getCertsDir() string {
	return common.TheDefaultClusterCertDir(k.getClusterName())
}

func (k *K3sRuntime) sendRegistryCertAndKey() error {
	return k.sendFileToHosts(k.GetMasterIPList()[:1], k.getCertsDir(), filepath.Join(k.getRootfs(), "certs"))
}

func (k *K3sRuntime) sendFileToHosts(Hosts []string, src, dst string) error {
	eg, _ := errgroup.WithContext(context.Background())
	for _, node := range Hosts {
		node := node
		eg.Go(func() error {
			ssh, err := k.getHostSSHClient(node)
			if err != nil {
				return fmt.Errorf("send file failed %v", err)
			}
			if err := ssh.Copy(node, src, dst); err != nil {
				return fmt.Errorf("send file failed %v", err)
			}
			return err
		})
	}
	return eg.Wait()
}

func (k *K3sRuntime) sendRegistryCert(host []string) error {
	cf := k.RegConfig
	err := k.sendFileToHosts(host, fmt.Sprintf("%s/%s.crt", k.getCertsDir(), cf.Domain), fmt.Sprintf("%s/%s:%s/%s.crt", DockerCertDir, cf.Domain, cf.Port, cf.Domain))
	if err != nil {
		return err
	}
	return k.sendFileToHosts(host, fmt.Sprintf("%s/%s.crt", k.getCertsDir(), cf.Domain), fmt.Sprintf("%s/%s:%s/%s.crt", DockerCertDir, SeaHub, cf.Port, cf.Domain))
}

func (k *K3sRuntime) ApplyRegistry() error {
	ssh, err := k.getHostSSHClient(k.RegConfig.IP)
	if err != nil {
		return fmt.Errorf("failed to get registry ssh client: %v", err)
	}

	if k.RegConfig.Username != "" && k.RegConfig.Password != "" {
		htpasswd, err := k.RegConfig.GenerateHtPasswd()
		if err != nil {
			return err
		}
		err = ssh.CmdAsync(k.RegConfig.IP, fmt.Sprintf("echo '%s' > %s", htpasswd, filepath.Join(k.getRootfs(), "etc", DefaultRegistryHtPasswdFile)))
		if err != nil {
			return err
		}
	}
	initRegistry := fmt.Sprintf("cd %s/scripts && sh init-registry.sh %s %s %s", k.getRootfs(), k.RegConfig.Port, fmt.Sprintf("%s/registry", k.getRootfs()), k.RegConfig.Domain)
	registryHost := k.getRegistryHost()
	addRegistryHosts := fmt.Sprintf(RemoteAddEtcHosts, registryHost, registryHost)
	if k.RegConfig.Domain != SeaHub {
		addSeaHubHosts := fmt.Sprintf(RemoteAddEtcHosts, k.RegConfig.IP+" "+SeaHub, k.RegConfig.IP+" "+SeaHub)
		addRegistryHosts = fmt.Sprintf("%s && %s", addRegistryHosts, addSeaHubHosts)
	}
	if err = ssh.CmdAsync(k.RegConfig.IP, initRegistry); err != nil {
		return err
	}
	if err = ssh.CmdAsync(k.GetMaster0IP(), addRegistryHosts); err != nil {
		return err
	}
	if k.RegConfig.Username == "" || k.RegConfig.Password == "" {
		return nil
	}
	return ssh.CmdAsync(k.GetMaster0IP(), k.GerLoginCommand())
}

func (k *K3sRuntime) InitMaster0() error {
	//cp -f %s/bin/k3s /usr/local/bin/
	//docker load < %s/images/k3s-airgap-images.tar.gz
	//kubectl create ns aihub
	cmd := fmt.Sprintf(`
INSTALL_K3S_SKIP_DOWNLOAD=true INSTALL_K3S_SELINUX_WARN=true INSTALL_K3S_EXEC='--docker --kubelet-arg cgroup-driver=systemd' %s/scripts/installk3s.sh
`, k.getRootfs())
	sshClient, err := k.getHostSSHClient(k.GetMaster0IP())
	if err != nil {
		return err
	}
	return sshClient.CmdAsync(k.GetMaster0IP(), cmd)
}
