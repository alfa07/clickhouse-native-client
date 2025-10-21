#!/usr/bin/env python3
"""
Fix lifetime issues in integration test files.
Replaces patterns like:
    let result_col = blocks[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnType>()
        .expect("Invalid column type");

With:
    let blocks = result.blocks();
    let col_ref = blocks[0].column(0).expect("Column not found");
    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnType>()
        .expect("Invalid column type");
"""

import re
import sys
from pathlib import Path

def fix_file(filepath):
    content = filepath.read_text()
    original = content

    # Pattern 1: Basic blocks[0].column pattern
    # Match: let result_col = blocks[0]\n.column(0)\n...
    pattern1 = re.compile(
        r'(\s+)(let result_col = blocks\[0\])\s*\n'
        r'\s*\.column\((\d+)\)\s*\n'
        r'\s*\.expect\("Column not found"\)\s*\n'
        r'\s*\.as_any\(\)\s*\n'
        r'\s*\.downcast_ref::<([^>]+)>\(\)\s*\n'
        r'\s*\.expect\("Invalid column type"\);',
        re.MULTILINE
    )

    def replace1(match):
        indent = match.group(1)
        col_idx = match.group(3)
        col_type = match.group(4)
        return (
            f'{indent}let blocks = result.blocks();\n'
            f'{indent}let col_ref = blocks[0].column({col_idx}).expect("Column not found");\n'
            f'{indent}let result_col = col_ref\n'
            f'{indent}    .as_any()\n'
            f'{indent}    .downcast_ref::<{col_type}>()\n'
            f'{indent}    .expect("Invalid column type");'
        )

    content = pattern1.sub(replace1, content)

    # Pattern 2: result.blocks()[0].column pattern
    # Match: let result_col = result.blocks()[0]\n.column(0)\n...
    pattern2 = re.compile(
        r'(\s+)(let result_col = result\.blocks\(\)\[0\])\s*\n'
        r'\s*\.column\((\d+)\)\s*\n'
        r'\s*\.expect\("Column not found"\)\s*\n'
        r'\s*\.as_any\(\)\s*\n'
        r'\s*\.downcast_ref::<([^>]+)>\(\)\s*\n'
        r'\s*\.expect\("Invalid column type"\);',
        re.MULTILINE
    )

    def replace2(match):
        indent = match.group(1)
        col_idx = match.group(3)
        col_type = match.group(4)
        return (
            f'{indent}let blocks = result.blocks();\n'
            f'{indent}let col_ref = blocks[0].column({col_idx}).expect("Column not found");\n'
            f'{indent}let result_col = col_ref\n'
            f'{indent}    .as_any()\n'
            f'{indent}    .downcast_ref::<{col_type}>()\n'
            f'{indent}    .expect("Invalid column type");'
        )

    content = pattern2.sub(replace2, content)

    if content != original:
        filepath.write_text(content)
        print(f"Fixed: {filepath.name}")
        return True
    return False

def main():
    test_dir = Path("tests")

    # Fix nullable and lowcardinality test files
    files_to_fix = list(test_dir.glob("integration_block_nullable*.rs")) + \
                   list(test_dir.glob("integration_block_lowcardinality*.rs"))

    fixed_count = 0
    for filepath in sorted(files_to_fix):
        if fix_file(filepath):
            fixed_count += 1

    print(f"\nFixed {fixed_count} files")

if __name__ == "__main__":
    main()
