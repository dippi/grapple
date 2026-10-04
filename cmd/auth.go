package cmd

import (
	"fmt"
	"os"

	"github.com/dippi/grapple/internal/auth"
	"github.com/spf13/cobra"
)

var authCmd = &cobra.Command{
	Use:   "auth",
	Short: "Inspect Grapple's authentication methods",
}

var authStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show detected authentication methods and how Grapple would authenticate",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		opts := authOptions(cmd)
		cobra.CheckErr(auth.ValidateMode(opts.Mode))
		status := auth.Inspect(cmd.Context(), opts)
		fmt.Print(status.Render())
		if status.Active == "" {
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(authCmd)
	authCmd.AddCommand(authStatusCmd)
}
