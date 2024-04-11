package tempodb

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/stretchr/testify/require"
)

func TestShardingBlockSelector(t *testing.T) {
	var (
		now        = time.Now()
		timeWindow = 12 * time.Hour
		tenantID   = ""

		// Reusing block IDs
		uuid1 = uuid.MustParse("00000000-0000-0000-0000-000000000001")
		uuid2 = uuid.MustParse("00000000-0000-0000-0000-000000000002")
		uuid3 = uuid.MustParse("00000000-0000-0000-0000-000000000003")
		uuid4 = uuid.MustParse("00000000-0000-0000-0000-000000000004")
		uuid5 = uuid.MustParse("00000000-0000-0000-0000-000000000005")

		// Reusing trace IDs
		id1 = []byte{1}
		id2 = []byte{2}
		id3 = []byte{3}
		id4 = []byte{4}
		id5 = []byte{5}

		// Various hash strings
		// hashTenantWindow           = fmt.Sprintf("%v-%v", tenantID, now.Unix())
		hashTenantOldWindowShardLevel0 = fmt.Sprintf("%v-%v-%03d-%03d", tenantID, now.Add(-timeWindow).Unix(), 0, 0)
		hashTenantWindowShardLevel0    = fmt.Sprintf("%v-%v-%03d-%03d", tenantID, now.Unix(), 0, 0)
		hashTenantWindowShard          = fmt.Sprintf("%v-%v-%03d", tenantID, now.Unix(), 8)
		hashTenantWindowShardLevel     = fmt.Sprintf("%v-%v-%03d-%03d", tenantID, now.Unix(), 8, 0) // shard=8 based on current config and logic
	)

	tests := []struct {
		name           string
		blocklist      []*backend.BlockMeta
		minInputBlocks int    // optional, defaults to global const
		maxInputBlocks int    // optional, defaults to global const
		maxBlockBytes  uint64 // optional, defaults to ???
		expected       []*backend.BlockMeta
		expectedHash   string
		expectedSecond []*backend.BlockMeta
		expectedHash2  string
	}{
		{
			name:      "nil - nil",
			blocklist: nil,
			expected:  nil,
		},
		{
			name:      "empty - nil",
			blocklist: []*backend.BlockMeta{},
			expected:  nil,
		},
		{
			name: "only two",
			blocklist: []*backend.BlockMeta{
				{
					BlockID: uuid1,
				},
				{
					BlockID: uuid2,
				},
			},
			expected: []*backend.BlockMeta{
				{
					BlockID: uuid1,
				},
				{
					BlockID: uuid2,
				},
			},
			expectedHash: hashTenantWindowShardLevel0,
		},
		{
			name: "choose two with lowest trace ID",
			blocklist: []*backend.BlockMeta{
				{
					BlockID: uuid2,
					MinID:   id2,
				},
				{
					BlockID: uuid3,
					MinID:   id3,
				},
				{
					BlockID: uuid1,
					MinID:   id1,
				},
			},
			maxInputBlocks: 2,
			expected: []*backend.BlockMeta{
				{
					BlockID: uuid1,
					MinID:   id1,
				},
				{
					BlockID: uuid2,
					MinID:   id2,
				},
			},
			expectedHash: hashTenantWindowShardLevel,
		},
		{
			name: "different windows",
			blocklist: []*backend.BlockMeta{
				{
					BlockID: uuid2,
					EndTime: now,
				},
				{
					BlockID: uuid1,
					EndTime: now.Add(-timeWindow),
				},
				{
					BlockID: uuid3,
					EndTime: now,
				},
				{
					BlockID: uuid4,
					EndTime: now.Add(-timeWindow),
				},
			},
			expected: []*backend.BlockMeta{
				{
					BlockID: uuid2,
					EndTime: now,
				},
				{
					BlockID: uuid3,
					EndTime: now,
				},
			},
			expectedHash: hashTenantWindowShardLevel0,
			expectedSecond: []*backend.BlockMeta{
				{
					BlockID: uuid1,
					EndTime: now.Add(-timeWindow),
				},
				{
					BlockID: uuid4,
					EndTime: now.Add(-timeWindow),
				},
			},
			expectedHash2: hashTenantOldWindowShardLevel0,
		},
		{
			// All of these blocks fall within the same shard.
			// Therefore each pass it will choose the two with the next lowest trace IDs.
			name: "different minimum trace ids",
			blocklist: []*backend.BlockMeta{
				{
					BlockID: uuid4,
					MinID:   id4,
				},
				{
					BlockID: uuid2,
					MinID:   id2,
				},
				{
					BlockID: uuid3,
					MinID:   id3,
				},
				{
					BlockID: uuid1,
					MinID:   id1,
				},
			},
			maxInputBlocks: 2,
			expected: []*backend.BlockMeta{
				{
					BlockID: uuid1,
					MinID:   id1,
				},
				{
					BlockID: uuid2,
					MinID:   id2,
				},
			},
			expectedHash: hashTenantWindowShardLevel,
			expectedSecond: []*backend.BlockMeta{
				{
					BlockID: uuid3,
					MinID:   id3,
				},
				{
					BlockID: uuid4,
					MinID:   id4,
				},
			},
			expectedHash2: hashTenantWindowShardLevel,
		},
		{
			// The two blocks that are already compacted and within a single shard (min/max=0)
			// will be prioritized over the 2 new blocks with CompactionLevel=0
			name: "different compaction lvls",
			blocklist: []*backend.BlockMeta{
				{
					BlockID:         uuid2,
					CompactionLevel: 1,
					MinID:           id2,
				},
				{
					BlockID: uuid1,
					MinID:   id1,
				},
				{
					BlockID:         uuid3,
					CompactionLevel: 1,
					MinID:           id3,
				},
				{
					BlockID: uuid4,
					MinID:   id4,
				},
			},
			expected: []*backend.BlockMeta{
				{
					BlockID:         uuid2,
					CompactionLevel: 1,
					MinID:           id2,
				},
				{
					BlockID:         uuid3,
					CompactionLevel: 1,
					MinID:           id3,
				},
			},
			expectedHash: hashTenantWindowShard,
			expectedSecond: []*backend.BlockMeta{
				{
					BlockID: uuid1,
					MinID:   id1,
				},
				{
					BlockID: uuid4,
					MinID:   id4,
				},
			},
			expectedHash2: hashTenantWindowShardLevel,
		},
		{
			name: "doesn't choose across time windows for already sharded blocks",
			blocklist: []*backend.BlockMeta{
				{
					BlockID:         uuid1,
					EndTime:         now,
					CompactionLevel: 1,
				},
				{
					BlockID:         uuid2,
					EndTime:         now.Add(-timeWindow),
					CompactionLevel: 1,
				},
			},
			expected:       nil,
			expectedHash:   "",
			expectedSecond: nil,
			expectedHash2:  "",
		},
		{
			name: "doesn't exceed max compaction objects",
			blocklist: []*backend.BlockMeta{
				{
					BlockID:         uuid1,
					TotalObjects:    99,
					CompactionLevel: 1,
				},
				{
					BlockID:         uuid2,
					TotalObjects:    2,
					CompactionLevel: 1,
				},
			},
			expected:       nil,
			expectedHash:   "",
			expectedSecond: nil,
			expectedHash2:  "",
		},
		{
			name: "doesn't exceed max block size",
			blocklist: []*backend.BlockMeta{
				{
					BlockID:         uuid1,
					Size:            50,
					CompactionLevel: 1,
				},
				{
					BlockID:         uuid2,
					Size:            51,
					CompactionLevel: 1,
				},
			},
			maxBlockBytes:  100,
			expected:       nil,
			expectedHash:   "",
			expectedSecond: nil,
			expectedHash2:  "",
		},
		{
			name: "Returns as many blocks as possible without exceeding max compaction objects",
			blocklist: []*backend.BlockMeta{
				{
					BlockID:         uuid1,
					TotalObjects:    50,
					CompactionLevel: 1,
				},
				{
					BlockID:         uuid2,
					TotalObjects:    50,
					CompactionLevel: 1,
				},
				{
					BlockID:         uuid3,
					TotalObjects:    50,
					CompactionLevel: 1,
				},
			},
			expected: []*backend.BlockMeta{
				{
					BlockID:         uuid1,
					TotalObjects:    50,
					CompactionLevel: 1,
				},
				{
					BlockID:         uuid2,
					TotalObjects:    50,
					CompactionLevel: 1,
				},
			},
			expectedHash:   hashTenantWindowShardLevel0,
			expectedSecond: nil,
			expectedHash2:  "",
		},
		{
			name:          "Returns as many blocks as possible without exceeding max block size",
			maxBlockBytes: 100,
			blocklist: []*backend.BlockMeta{
				{
					BlockID:         uuid1,
					Size:            50,
					CompactionLevel: 1,
				},
				{
					BlockID:         uuid2,
					Size:            50,
					CompactionLevel: 1,
				},
				{
					BlockID:         uuid3,
					Size:            1,
					CompactionLevel: 1,
				},
			},
			expected: []*backend.BlockMeta{
				{
					BlockID:         uuid1,
					Size:            50,
					CompactionLevel: 1,
				},
				{
					BlockID:         uuid2,
					Size:            50,
					CompactionLevel: 1,
				},
			},
			expectedHash:   hashTenantWindowShardLevel0,
			expectedSecond: nil,
			expectedHash2:  "",
		},
		{
			// First compaction gets 3 blocks, second compaction gets 2 more
			// Blocks are all same shard and level so they are sorted by min id
			name:           "choose more than 2 blocks",
			maxInputBlocks: 3,
			blocklist: []*backend.BlockMeta{
				{
					BlockID: uuid1,
					MinID:   id1,
				},
				{
					BlockID: uuid2,
					MinID:   id2,
				},
				{
					BlockID: uuid4,
					MinID:   id4,
				},
				{
					BlockID: uuid5,
					MinID:   id5,
				},
				{
					BlockID: uuid3,
					MinID:   id3,
				},
			},
			expected: []*backend.BlockMeta{
				{
					BlockID: uuid1,
					MinID:   id1,
				},
				{
					BlockID: uuid2,
					MinID:   id2,
				},
				{
					BlockID: uuid3,
					MinID:   id3,
				},
			},
			expectedHash: hashTenantWindowShardLevel,
			expectedSecond: []*backend.BlockMeta{
				{
					BlockID: uuid4,
					MinID:   id4,
				},
				{
					BlockID: uuid5,
					MinID:   id5,
				},
			},
			expectedHash2: hashTenantWindowShardLevel,
		},
		{
			name: "honors minimum block count for sharded blocks",
			blocklist: []*backend.BlockMeta{
				{
					BlockID:         uuid1,
					CompactionLevel: 1,
				},
				{
					BlockID:         uuid2,
					CompactionLevel: 1,
				},
			},
			minInputBlocks: 3,
			maxInputBlocks: 3,
			expected:       nil,
			expectedHash:   "",
			expectedSecond: nil,
			expectedHash2:  "",
		},
		{
			name: "don't compact across dataEncodings",
			blocklist: []*backend.BlockMeta{
				{
					BlockID:         uuid1,
					DataEncoding:    "bar",
					CompactionLevel: 1,
				},
				{
					BlockID:         uuid2,
					DataEncoding:    "foo",
					CompactionLevel: 1,
				},
			},
			expected: nil,
		},
		{
			name: "don't compact across versions",
			blocklist: []*backend.BlockMeta{
				{
					BlockID:         uuid1,
					Version:         "v2",
					CompactionLevel: 1,
				},
				{
					BlockID:         uuid2,
					Version:         "vParquet",
					CompactionLevel: 1,
				},
			},
			expected:       nil,
			expectedHash:   "",
			expectedSecond: nil,
			expectedHash2:  "",
		},
		{
			name: "ensures blocks of the same version are compacted",
			blocklist: []*backend.BlockMeta{
				{
					BlockID: uuid1,
					Version: "v2",
				},
				{
					BlockID: uuid2,
					Version: "vParquet",
				},
				{
					BlockID: uuid3,
					Version: "v2",
				},
				{
					BlockID: uuid4,
					Version: "vParquet",
				},
			},
			expected: []*backend.BlockMeta{
				{
					BlockID: uuid1,
					Version: "v2",
				},
				{
					BlockID: uuid3,
					Version: "v2",
				},
			},
			expectedHash: hashTenantWindowShardLevel0,
			expectedSecond: []*backend.BlockMeta{
				{
					BlockID: uuid2,
					Version: "vParquet",
				},
				{
					BlockID: uuid4,
					Version: "vParquet",
				},
			},
			expectedHash2: hashTenantWindowShardLevel0,
		},
		{
			name: "blocks with different dedicated columns are not selected together",
			blocklist: []*backend.BlockMeta{
				{
					BlockID: uuid1,
					DedicatedColumns: backend.DedicatedColumns{
						{Scope: "span", Name: "foo", Type: "int"},
					},
				},
				{
					BlockID: uuid2,
					DedicatedColumns: backend.DedicatedColumns{
						{Scope: "span", Name: "foo", Type: "string"},
					},
				},
				{
					BlockID: uuid3,
					DedicatedColumns: backend.DedicatedColumns{
						{Scope: "span", Name: "foo", Type: "int"},
					},
				},
				{
					BlockID: uuid4,
					DedicatedColumns: backend.DedicatedColumns{
						{Scope: "span", Name: "foo", Type: "string"},
					},
				},
			},
			expected: []*backend.BlockMeta{
				{
					BlockID: uuid1,
					DedicatedColumns: backend.DedicatedColumns{
						{Scope: "span", Name: "foo", Type: "int"},
					},
				},
				{
					BlockID: uuid3,
					DedicatedColumns: backend.DedicatedColumns{
						{Scope: "span", Name: "foo", Type: "int"},
					},
				},
			},
			expectedHash: hashTenantWindowShardLevel0,
			expectedSecond: []*backend.BlockMeta{
				{
					BlockID: uuid2,
					DedicatedColumns: backend.DedicatedColumns{
						{Scope: "span", Name: "foo", Type: "string"},
					},
				},
				{
					BlockID: uuid4,
					DedicatedColumns: backend.DedicatedColumns{
						{Scope: "span", Name: "foo", Type: "string"},
					},
				},
			},
			expectedHash2: hashTenantWindowShardLevel0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			min := DefaultMinInputBlocks
			if tt.minInputBlocks > 0 {
				min = tt.minInputBlocks
			}

			max := DefaultMaxInputBlocks
			if tt.maxInputBlocks > 0 {
				max = tt.maxInputBlocks
			}

			maxSize := uint64(1024 * 1024)
			if tt.maxBlockBytes > 0 {
				maxSize = tt.maxBlockBytes
			}

			// To simplify tests we fixup all of the following:

			fix := func(m *backend.BlockMeta) {
				// Assume block is current window unless specified
				if m.EndTime.IsZero() {
					m.EndTime = now
				}
				// Assume each block has 1 trace and min==max
				if len(m.MaxID) == 0 {
					m.MaxID = m.MinID
				}
			}

			for _, b := range tt.blocklist {
				fix(b)
			}
			for _, b := range tt.expected {
				fix(b)
			}
			for _, b := range tt.expectedSecond {
				fix(b)
			}

			selector := newShardingBlockSelector(2, tt.blocklist, time.Second, 100, maxSize, min, max)

			actual := selector.BlocksToCompact()

			// Repair

			require.Equal(t, tt.expected, actual.Blocks())
			require.Equal(t, tt.expectedHash, actual.Ownership())

			actual = selector.BlocksToCompact()
			require.Equal(t, tt.expectedSecond, actual.Blocks())
			require.Equal(t, tt.expectedHash2, actual.Ownership())
		})
	}
}

/*func TestRealIndex(t *testing.T) {
	x, err := os.ReadFile("/Users/marty/src/deployment_tools/index.json")
	require.NoError(t, err)

	i := backend.TenantIndex{}

	err = json.Unmarshal(x, &i)
	require.NoError(t, err)

	window := 15 * time.Minute
	maxObjs := 3_000_000
	maxSize := uint64(107374182400)
	selector := newShardingBlockSelector(1, i.Meta, window, maxObjs, maxSize, 2, 4)

	for {
		cmd := selector.BlocksToCompact()
		if len(cmd.Blocks()) == 0 {
			break
		}

		// fmt.Println("Compaction command:", blockIDs, cmd.Ownership())
	}
}*/
