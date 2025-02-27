// Copyright 2025 California Institute of Technology (Caltech)
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

package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/genelet/determined/convert"
	"github.com/spf13/cobra"
	"log"
	"os"
	"strings"
)

var rootCmd = &cobra.Command{
	Use:   "tfvars2json tfvars_file output_json",
	Short: "Convert terraform variables file to JSON",
	Run:   hcl2json,
	Args:  cobra.ExactArgs(2),
}

func init() {
	rootCmd.Flags().Bool("overwrite", false, "Overwrite output file if it exists")
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func hcl2json(cmd *cobra.Command, args []string) {
	varsFile := args[0]

	_, err := os.Stat(varsFile)

	if err != nil {
		log.Fatal(err)
	}

	hclData, err := os.ReadFile(varsFile)

	if err != nil {
		log.Fatal(err)
	}

	jsonData, err := convert.HCLToJSON(hclData)

	if err != nil {
		log.Fatal(err)
	}

	var prettyJSON bytes.Buffer
	err = json.Indent(&prettyJSON, jsonData, "", "    ")

	if err == nil {
		jsonData = prettyJSON.Bytes()
	}

	outFile := args[1]

	if outFile != "-" {
		if !strings.HasSuffix(outFile, ".json") {
			outFile = outFile + ".json"
		}

		overwrite, err := cmd.Flags().GetBool("overwrite")

		if err != nil {
			overwrite = false
		}

		outStat, err := os.Stat(outFile)

		var canWrite bool

		if err != nil && errors.Is(err, os.ErrNotExist) {
			canWrite = true
		} else if err == nil {
			if outStat.IsDir() {
				log.Fatal("Output path is a directory")
			}

			canWrite = overwrite
		} else {
			log.Fatal(err)
		}

		if !canWrite {
			log.Fatal("Cannot write output as a file with that name already exists. Use --overwrite to write anyway")
		}

		err = os.WriteFile(outFile, jsonData, 0666)

		if err != nil {
			log.Fatalf("failed to write output file: %v\n", err)
		}
	} else {
		fmt.Println(string(jsonData))
	}
}
