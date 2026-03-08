// SPDX-FileCopyrightText: 2024 The margaret Authors
//
// SPDX-License-Identifier: MIT

// Command roaring-stats inspects a filesystem-backed roaring bitmap multilog
// directory and prints statistics about bitmap sizes, cardinalities, densities,
// and storage overhead.
//
// Usage:
//
//	roaring-stats <path> [<path> ...]
//
// Each path should be the root directory of a roaring multilog that uses the
// filesystem persist backend (one file per serialized bitmap, hex-encoded filenames).
package main

import (
	"cmp"
	"fmt"
	iofs "io/fs"
	"os"
	"path/filepath"
	"slices"
	"text/tabwriter"

	"github.com/ssbc/margaret/v2/internal/persist/fs"
	"github.com/ssbc/margaret/v2/multilog/roaring"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: roaring-stats <multilog-dir> [<multilog-dir> ...]\n")
		os.Exit(1)
	}

	for _, path := range os.Args[1:] {
		if err := inspect(path); err != nil {
			fmt.Fprintf(os.Stderr, "error inspecting %s: %v\n", path, err)
			os.Exit(1)
		}
	}
}

func inspect(path string) error {
	store := fs.New(path)
	ml := roaring.NewStore(store)

	// Load all sublogs.
	addrs, err := ml.List()
	if err != nil {
		ml.Close()
		return fmt.Errorf("list: %w", err)
	}

	// Ensure all are loaded for stats.
	for _, addr := range addrs {
		if _, err := ml.Get(addr); err != nil {
			ml.Close()
			return fmt.Errorf("get %s: %w", addr, err)
		}
	}

	stats := ml.Stats()
	ml.Close()

	// On-disk size.
	diskSize, err := dirSize(path)
	if err != nil {
		return fmt.Errorf("disk size: %w", err)
	}

	fmt.Printf("\n=== %s ===\n", path)
	fmt.Printf("  sublogs:           %d\n", stats.NumSublogs)
	fmt.Printf("  total entries:     %d\n", stats.TotalCardinality)
	fmt.Printf("  avg entries:       %.0f  (min=%d, max=%d)\n", stats.AvgCardinality, stats.MinCardinality, stats.MaxCardinality)
	fmt.Printf("  avg density:       %.4f\n", stats.AvgDensity)
	fmt.Printf("  avg bitmap size:   %.0f B  (min=%d, max=%d)\n", stats.AvgSerializedSize, stats.MinSerializedSize, stats.MaxSerializedSize)
	fmt.Printf("  total bitmap data: %s\n", humanBytes(stats.TotalSerializedSize))
	fmt.Printf("  disk size:         %s\n", humanBytes(diskSize))
	if stats.TotalSerializedSize > 0 {
		fmt.Printf("  overhead:          %.2fx\n", float64(diskSize)/float64(stats.TotalSerializedSize))
	}

	// Print per-sublog details, sorted by cardinality descending.
	slices.SortFunc(stats.Sublogs, func(a, b roaring.SublogStats) int {
		return cmp.Compare(b.Cardinality, a.Cardinality) // descending
	})

	fmt.Println()
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "  ADDR\tCARD\tBITMAP SIZE\tMIN\tMAX\tDENSITY\n")
	fmt.Fprintf(tw, "  ----\t----\t-----------\t---\t---\t-------\n")

	shown := 0
	for _, ss := range stats.Sublogs {
		if shown >= 20 && stats.NumSublogs > 25 {
			fmt.Fprintf(tw, "  ... and %d more sublogs\t\t\t\t\t\n", stats.NumSublogs-shown)
			break
		}
		addrStr := string(ss.Addr)
		if len(addrStr) > 40 {
			addrStr = addrStr[:37] + "..."
		}
		fmt.Fprintf(tw, "  %s\t%d\t%s\t%d\t%d\t%.4f\n",
			addrStr, ss.Cardinality, humanBytes(int64(ss.SerializedSize)),
			ss.MinValue, ss.MaxValue, ss.Density)
		shown++
	}
	tw.Flush()
	fmt.Println()

	// Histogram of cardinalities.
	if stats.NumSublogs > 0 {
		printHistogram("cardinality", stats.Sublogs, func(s roaring.SublogStats) int { return s.Cardinality })
		printHistogram("bitmap size (bytes)", stats.Sublogs, func(s roaring.SublogStats) int { return s.SerializedSize })
	}

	return nil
}

func printHistogram(label string, sublogs []roaring.SublogStats, val func(roaring.SublogStats) int) {
	// Find range.
	minV, maxV := val(sublogs[0]), val(sublogs[0])
	for _, s := range sublogs {
		v := val(s)
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}

	if minV == maxV {
		fmt.Printf("  %s: all = %d\n\n", label, minV)
		return
	}

	// 10 buckets.
	const numBuckets = 10
	bucketSize := (maxV - minV + numBuckets) / numBuckets
	buckets := make([]int, numBuckets)
	for _, s := range sublogs {
		idx := (val(s) - minV) / bucketSize
		if idx >= numBuckets {
			idx = numBuckets - 1
		}
		buckets[idx]++
	}

	maxCount := 0
	for _, c := range buckets {
		if c > maxCount {
			maxCount = c
		}
	}

	fmt.Printf("  %s distribution:\n", label)
	for i, count := range buckets {
		lo := minV + i*bucketSize
		hi := lo + bucketSize - 1
		if i == numBuckets-1 {
			hi = maxV
		}
		barLen := 0
		if maxCount > 0 {
			barLen = count * 40 / maxCount
		}
		bar := ""
		for range barLen {
			bar += "#"
		}
		fmt.Printf("    %8d - %8d  [%4d] %s\n", lo, hi, count, bar)
	}
	fmt.Println()
}

func humanBytes(b int64) string {
	switch {
	case b >= 1024*1024:
		return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, d iofs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}
