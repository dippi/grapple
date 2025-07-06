package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/dippi/grapple/internal/logadmin"
	"github.com/googleapis/gax-go/v2/apierror"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

var cliName = "grapple"
var cfgFile string

var rootCmd = &cobra.Command{
	Use:   cliName,
	Short: "Fetch logs from Google Cloud Logging",
	Long:  `Fetch logs from Google Cloud Logging`,
	Args:  cobra.MatchAll(cobra.MaximumNArgs(1), cobra.OnlyValidArgs),
	Run: func(cmd *cobra.Command, args []string) {
		projectId := viper.GetString("project")
		if projectId == "" {
			log.Fatal("Error: required flag \"project\" not set")
		}

		from, to, err := determineTimeWindow(cmd)
		cobra.CheckErr(err)

		filter := ""
		if len(args) > 0 {
			filter = args[0]
		}
		allFilters := buildFilter(from, to, filter)

		newestFirst := viper.GetString("order") == "desc"

		ctx := cmd.Context()

		client, err := logadmin.NewClient(ctx, projectId)
		cobra.CheckErr(err)
		defer client.Close()

		opts := []logadmin.EntriesOption{
			logadmin.PageSize(1000),
			logadmin.Filter(allFilters),
		}

		if newestFirst {
			opts = append(opts, logadmin.NewestFirst())
		}

		err = fetchAndProcessLogs(ctx, client, opts)
		cobra.CheckErr(err)
	},
}

func Execute() {
	log.SetFlags(0)
	err := rootCmd.Execute()
	cobra.CheckErr(err)
}

func init() {
	cobra.OnInitialize(initConfig)

	configDescription := fmt.Sprintf("config file (default is .%v.yaml in the working directory or in the home directory)", cliName)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", configDescription)

	rootCmd.Flags().String("project", "", "Google Cloud Platform project ID")
	rootCmd.Flags().String("from", "", "start of time range")
	rootCmd.Flags().String("to", "", "end of time range")
	rootCmd.Flags().String("freshness", "", "maximum age of log entries (e.g. 2h, 3d4h)")
	rootCmd.Flags().String("order", "desc", "ordering based on timestamp, valid values: asc, desc")

	rootCmd.MarkFlagFilename("config")

	viper.BindPFlag("project", rootCmd.Flags().Lookup("project"))
	viper.BindPFlag("order", rootCmd.Flags().Lookup("order"))
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		viper.AddConfigPath(home)
		viper.AddConfigPath(".")
		viper.SetConfigType("yaml")
		viper.SetConfigName(fmt.Sprintf(".%v", cliName))
	}

	viper.SetEnvPrefix(cliName)
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		log.Println("Using config file:", viper.ConfigFileUsed())
	} else if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
		log.Fatalf("Error: %v", err)
	}
}

// determineTimeWindow parses time-related flags and returns the appropriate time range
func determineTimeWindow(cmd *cobra.Command) (from, to time.Time, err error) {
	freshness := cmd.Flag("freshness").Value.String()

	fromFlag := cmd.Flag("from").Value.String()
	toFlag := cmd.Flag("to").Value.String()

	if freshness != "" {
		if fromFlag != "" || toFlag != "" {
			return from, to, errors.New("--freshness cannot be used together with --from or --to")
		}

		dur, err := parseFreshness(freshness)
		if err != nil {
			return from, to, err
		}
		to = time.Now()
		from = to.Add(-dur)
		return from, to, nil
	}

	if fromFlag != "" && toFlag != "" {
		from, err = time.Parse(time.RFC3339, fromFlag)
		if err != nil {
			return from, to, fmt.Errorf("invalid --from: %w", err)
		}
		to, err = time.Parse(time.RFC3339, toFlag)
		if err != nil {
			return from, to, fmt.Errorf("invalid --to: %w", err)
		}
		return from, to, nil
	} else if fromFlag == "" && toFlag == "" {
		// No explicit time window, logadmin will apply its default.
		return from, to, nil
	} else {
		return from, to, errors.New("either specify both --from and --to, or neither")
	}
}

// parseFreshness converts strings like "1d", "2h", "30m" into a time.Duration.
// "d" is interpreted as 24h.
func parseFreshness(expression string) (time.Duration, error) {
	if expression == "" {
		return 0, fmt.Errorf("invalid freshness %q", expression)
	}

	re := regexp.MustCompile(`^(?:(\d+)d)?(.*)$`)
	match := re.FindStringSubmatch(expression)

	var total time.Duration

	if match[1] != "" {
		days, err := strconv.Atoi(match[1])
		if err != nil {
			return 0, fmt.Errorf("invalid freshness %q", expression)
		}
		total += time.Duration(days) * 24 * time.Hour
	}

	if match[2] != "" {
		other, err := time.ParseDuration(match[2])
		if err != nil {
			return 0, fmt.Errorf("invalid freshness %q", expression)
		}
		total += other
	}

	return total, nil
}

// buildFilter combines time filter and user filter into a single filter string
func buildFilter(from, to time.Time, userFilter string) string {
	var timeFilter string
	if !from.IsZero() && !to.IsZero() {
		timeFilter = fmt.Sprintf(
			`timestamp >= %q AND timestamp <= %q`,
			from.Format(time.RFC3339),
			to.Format(time.RFC3339),
		)
	}

	if timeFilter == "" {
		return userFilter
	} else if userFilter == "" {
		return timeFilter
	}
	return fmt.Sprintf("(%s) AND %s", userFilter, timeFilter)
}

// handleRateLimitError processes rate limit errors and returns whether the operation was rate limited
func handleRateLimitError(err error, rateLimited bool) bool {
	// This could handled with status.FromError and err.Code() like the one below
	// with code ResourceExhausted, but it wouldn't give us easy access to the metadata.
	// Another way around would be to use status.FromError, then get the .Details()
	// cast "any" to "google.golang.org/genproto/googleapis/rpc/errdetails.ErrorInfo"
	// and get the metadata from there.
	if apiErr, ok := err.(*apierror.APIError); ok && apiErr.Reason() == "RATE_LIMIT_EXCEEDED" {
		if !rateLimited {
			metadata := apiErr.Metadata()
			quotaLimit := metadata["quota_limit"]
			quotaLimitValue := metadata["quota_limit_value"]
			if quotaLimit != "" && quotaLimitValue != "" {
				log.Printf("Rate limit exceeded (%s: %s), sleeping...", quotaLimit, quotaLimitValue)
			} else {
				log.Println("Rate limit exceeded, sleeping...")
				log.Println(apiErr)
			}
		} else {
			log.Println(".")
		}
		time.Sleep(1 * time.Second)
		return true
	}
	return false
}

// fetchAndProcessLogs fetches logs from the API and processes them
func fetchAndProcessLogs(ctx context.Context, client *logadmin.Client, opts []logadmin.EntriesOption) error {
	rateLimited := false
	currentToken := ""

outer:
	for {
		it := client.Entries(ctx, opts...)

		pager := iterator.NewPager(it, 1000, currentToken)
		for {
			var entries []*loggingpb.LogEntry
			nextToken, err := pager.NextPage(&entries)
			if err != nil {
				if errors.Is(err, context.Canceled) || err.Error() == "no more items in iterator" {
					break outer
				}
				if rateLimited = handleRateLimitError(err, rateLimited); rateLimited {
					break
				}
				if err, ok := status.FromError(err); ok && err.Code() == codes.Unauthenticated {
					return errors.New("unauthenticated, please run `gcloud auth application-default login` and try again")
				}
				return err
			}

			if rateLimited {
				log.Println("Rate limit expired")
				rateLimited = false
			}

			for _, entry := range entries {
				jsonBytes, err := protojson.MarshalOptions{Multiline: false}.Marshal(entry)
				if err != nil {
					log.Printf("Error marshaling log entry (%s): %v", entry.InsertId, err)
					continue
				}
				fmt.Println(string(jsonBytes))
			}

			if nextToken == "" {
				break outer
			}

			currentToken = nextToken
		}
	}
	return nil
}
