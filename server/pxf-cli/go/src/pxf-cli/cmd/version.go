package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Display the version of PXF and exit",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("PXF Version x.x.x")
	},
}
