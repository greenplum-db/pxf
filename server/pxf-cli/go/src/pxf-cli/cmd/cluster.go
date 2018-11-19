package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(clusterCmd)
}

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "perform <command> on each segment host in the cluster",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("cluster")
	},
}
