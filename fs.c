
#include "fs.h"
#include "disk.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <math.h>
#include <stdbool.h>

// Constants

#define FS_MAGIC           0xf0f03410
#define INODES_PER_BLOCK   128
#define POINTERS_PER_INODE 5
#define POINTERS_PER_BLOCK 1024

// Global Variables

bool is_mounted = false;
bool *free_block_bm;

// Data Structures

struct fs_superblock {
	int magic;
	int nblocks;
	int ninodeblocks;
	int ninodes;
};

struct fs_inode {
	int isvalid;
	int size;
	int direct[POINTERS_PER_INODE];
	int indirect;
};

union fs_block {
	struct fs_superblock super;
	struct fs_inode inode[INODES_PER_BLOCK];
	int pointers[POINTERS_PER_BLOCK];
	char data[DISK_BLOCK_SIZE];
};

// Low Level Functions (Helpers)

void inode_load( int inumber, struct fs_inode *inode ) {
	int block_number = 1 + floor(inumber / INODES_PER_BLOCK);
	int block_inode = inumber % INODES_PER_BLOCK;

	// Load the block containing the inode
	union fs_block block;
	disk_read(block_number, block.data);

	// Get the inode of interest
	*inode = block.inode[block_inode];
	return;
}

void inode_save( int inumber, struct fs_inode *inode ) {
	int block_number = 1 + floor(inumber / INODES_PER_BLOCK);
	int block_inode = inumber % INODES_PER_BLOCK;

	// Load the block containing the inode
	union fs_block block;
	disk_read(block_number, block.data);

	// Write the new inode
	block.inode[block_inode] = *inode;
	disk_write(block_number, block.data);
}

bool is_valid_inumber( int inumber ){
	// Load the super block
	union fs_block super_block;
	disk_read(0, super_block.data);

	// Check that the block number and block inode are within range
	if(inumber > 0 && inumber < super_block.super.ninodes){
		struct fs_inode inode;
		inode_load(inumber, &inode);
		// Check that the inode is valid
		if(inode.isvalid){
			return true;
		}else{
			return false;
		}
	}
	return false;
}

void dump_free_blocks(int nblocks){
	int i;
	for(i = 0; i < nblocks; i++){
		printf("%d", free_block_bm[i]);
	}
	printf("\n");
}

// High Level Functions

int fs_format(){
	// Check if the disk is mounted; if it is, do nothing and return failure
	if(is_mounted) return 0;

	// Set all inodes to be invalid if the disk
	union fs_block super_block;
	disk_read(0, super_block.data);
	if(super_block.super.magic == FS_MAGIC){
		int inode;
		for(inode = 0; inode < super_block.super.ninodeblocks; inode++){
			char new_block[DISK_BLOCK_SIZE] = {0};
			disk_write(inode + 1, new_block);
		}
	}

	// Create then write the new valid superblock
 	union fs_block new_super;
	new_super.super.magic = FS_MAGIC;
	new_super.super.nblocks = disk_size();
	new_super.super.ninodeblocks = ceil(disk_size() / 10.0);
	new_super.super.ninodes = new_super.super.ninodeblocks * INODES_PER_BLOCK;
	disk_write(0, new_super.data);

	return 1;
}

void fs_debug(){
	union fs_block super_block, block;

	// Super Block
	disk_read(0, super_block.data);
	printf("superblock:\n");
	if(super_block.super.magic == FS_MAGIC){
		printf("\tmagic number is valid\n");
	}else{
		printf("\tmagic number is NOT valid\n");
		return;
	}
	printf("\t%d blocks\n", super_block.super.nblocks);
	printf("\t%d inode blocks\n", super_block.super.ninodeblocks);
	printf("\t%d inodes\n", super_block.super.ninodes);

	// Scan for used inodes and report
	int inode_block;
	for(inode_block = 0; inode_block < super_block.super.ninodeblocks; inode_block++){
		disk_read(inode_block + 1, block.data);

		// Check each inode in the block and check if it is valid
		int inode;
		for(inode = 0; inode < INODES_PER_BLOCK; inode++){
			if(block.inode[inode].isvalid){
				printf("inode %d:\n", inode + (INODES_PER_BLOCK * inode_block));

				// Print out the size of the inode data
				int size = block.inode[inode].size;
				printf("\tsize: %d bytes\n", size);

				// Print out which blocks are pointed to:

				// Blocks pointed to by direct pointers
				if(size > 0){
					printf("\tdirect blocks:");
					int direct;
					for(direct = 0; direct < POINTERS_PER_INODE; direct++){
						if(size <= 0) break;
						if(block.inode[inode].direct[direct]){
							printf(" %d", block.inode[inode].direct[direct]);
							size -= DISK_BLOCK_SIZE;
						}
					}
					printf("\n");
				}
				// Indirect block
				if(size > 0 && block.inode[inode].indirect){
					printf("\tindirect block: %d\n", block.inode[inode].indirect);

					// Blocks pointed to by pointers in the indirect block
					printf("\tindirect data blocks:");
					union fs_block indirect_block;
					disk_read(block.inode[inode].indirect, indirect_block.data);
					int indirect;
					for(indirect = 0; indirect < POINTERS_PER_BLOCK; indirect++){
						if(size <= 0) break;
						if(indirect_block.pointers[indirect]){
							printf(" %d", indirect_block.pointers[indirect]);
							size -= DISK_BLOCK_SIZE;
						}
					}
					printf("\n");
				}
			}
		}
	}
}

int fs_mount(){
	// Check the disk for a file system
	union fs_block super_block, block;
	disk_read(0, super_block.data);
	if(super_block.super.magic != FS_MAGIC){
		return 0; // Disk does not have this file system
	}

	// Create a free block bitmap
	free_block_bm = malloc(super_block.super.nblocks * sizeof(bool));
	memset(free_block_bm, true, super_block.super.nblocks * sizeof(bool));
	free_block_bm[0] = false; // Super block always in use

	// Find which blocks are in use by checking direct and indirect pointers
	int inode_block;
	for(inode_block = 0; inode_block < super_block.super.ninodeblocks; inode_block++){
		free_block_bm[inode_block + 1] = false; // Inode blocks are not free
		disk_read(inode_block + 1, block.data);

		// Check each inode in the block and check if it is valid
		int inode;
		for(inode = 0; inode < INODES_PER_BLOCK; inode++){
			if(block.inode[inode].isvalid){
				int size = block.inode[inode].size;
				// Blocks pointed to by direct pointers
				int direct;
				for(direct = 0; direct < POINTERS_PER_INODE; direct++){
					if(size <= 0) break;
					if(block.inode[inode].direct[direct]){
						free_block_bm[block.inode[inode].direct[direct]] = false;
						size -= DISK_BLOCK_SIZE;
					}
				}
				// Indirect block
				if(size > 0 && block.inode[inode].indirect){
					free_block_bm[block.inode[inode].indirect] = false;

					// Blocks pointed to by pointers in the indirect block
					union fs_block indirect_block;
					disk_read(block.inode[inode].indirect, indirect_block.data);
					int indirect;
					for(indirect = 0; indirect < POINTERS_PER_BLOCK; indirect++){
						if(size <= 0) break;
						if(indirect_block.pointers[indirect]){
							free_block_bm[indirect_block.pointers[indirect]] = false;
							size -= DISK_BLOCK_SIZE;
						}
					}
				}
			}
		}
	}
	is_mounted = true;
	return 1;
}

int fs_create(){
	// Mount is a prequisite
	if(!is_mounted) return 0;

	// Place the inode in the first unused inumber
	union fs_block super_block;
	disk_read(0, super_block.data);
	struct fs_inode inode;
	int inumber;
	for(inumber = 1; inumber < super_block.super.ninodes; inumber++){
		inode_load(inumber, &inode);
		if(!inode.isvalid){ // Create and save the new inode in open spot
			inode.isvalid = 1;
			inode.size = 0;
			int ptr;
			for(ptr = 0; ptr < POINTERS_PER_INODE; ptr++){
				inode.direct[ptr] = 0;
			}
			inode_save(inumber, &inode);
			return inumber;
		}
	}
	// If it gets here, no empty inodes = failure
	return 0;
}

int fs_delete( int inumber ){
	// Mount is a prequisite and inumber must be in range of inodes
	if(!is_mounted || !is_valid_inumber(inumber)) return 0;

	// First mark all data and indirect blocks for this inode free
	struct fs_inode inode;
	inode_load(inumber, &inode);
	// Free direct pointers
	int direct;
	for(direct = 0; direct < POINTERS_PER_INODE; direct++){
		if(inode.size <= 0) break;
		if(inode.direct[direct]){
			free_block_bm[inode.direct[direct]] = true;
			inode.size -= DISK_BLOCK_SIZE;
		}
	}
	// Free indirect block and indirect pointer blocks
	if(inode.size > 0 && inode.indirect){
		free_block_bm[inode.indirect] = true;
		union fs_block indirect_block;
		disk_read(inode.indirect, indirect_block.data);
		int indirect;
		for(indirect = 0; indirect < POINTERS_PER_BLOCK; indirect++){
			if(inode.size <= 0) break;
			if(indirect_block.pointers[indirect]){
				free_block_bm[indirect_block.pointers[indirect]] = true;
				inode.size -= DISK_BLOCK_SIZE;
			}
		}
	}

	// Delete the inode by setting it invalid
	inode.isvalid = 0;
	inode_save(inumber, &inode);
	return 1;
}

int fs_getsize( int inumber ){
	// Mount is a prequisite and inumber must be in range of inodes
	if(!is_mounted || !is_valid_inumber(inumber)) return -1;

	// Load the inode and return its logical size
	struct fs_inode inode;
	inode_load(inumber, &inode);
	return inode.size;
}

int fs_read( int inumber, char *data, int length, int offset ){
	// Mount is a prequisite and inumber must be in range of inodes
	if(!is_mounted || !is_valid_inumber(inumber)) return 0;
	// Don't try to read anything if there is nothing to read or invalid offset
	if(length <= 0 || offset < 0) return 0;

	// Load the inode and size
	struct fs_inode inode;
	inode_load(inumber, &inode);
	int size = inode.size;
	if(offset >= size) return 0;

	// Create a list of the data blocks pointed to by the direct and indirect pointer
	union fs_block super_block, block;
	disk_read(0, super_block.data);
	bool *blocks = malloc(super_block.super.nblocks * sizeof(bool));
	memset(blocks, false, super_block.super.nblocks * sizeof(bool)); // Initialize to false
	int dptr;
	for(dptr = 0; dptr < POINTERS_PER_INODE; dptr++){ // Blocks by direct pointers
		if(size <= 0) break;
		if(inode.direct[dptr]){
			blocks[inode.direct[dptr]] = true;
			size -= DISK_BLOCK_SIZE;
		}
	}
	if(size > 0 && inode.indirect){
		union fs_block indirect_block;
		disk_read(inode.indirect, indirect_block.data);
		int iptr;
		for(iptr = 0; iptr < POINTERS_PER_BLOCK; iptr++){ // blocks by indirect
			if(size <= 0) break;
			if(indirect_block.pointers[iptr]){
				blocks[indirect_block.pointers[iptr]] = true;
				size -= DISK_BLOCK_SIZE;
			}
		}
	}
	size = inode.size;

	// Read the blocks that are found to be used by the inode
	int offset_ptr = floor(offset / DISK_BLOCK_SIZE);
	int read_counter = 0, n_block = 0, block_offset;
	int b;
	for(b = 0; b < super_block.super.nblocks; b++){
		if(length <= 0 || size <= 0) break;
		else if(blocks[b]){
			if(offset_ptr == n_block){
				block_offset = (offset - (offset_ptr * DISK_BLOCK_SIZE)) % DISK_BLOCK_SIZE;
				// Read the data block and add the appropriate part to the data
				disk_read(b, block.data);
				if(size < DISK_BLOCK_SIZE){
					memcpy(data + read_counter, block.data + block_offset, size);
					read_counter += size;
					break;
				}else if(block_offset + length < DISK_BLOCK_SIZE){
					memcpy(data + read_counter, block.data + block_offset, length);
					read_counter += length;
					break;
				}else{
					memcpy(data + read_counter, block.data + block_offset, DISK_BLOCK_SIZE - block_offset);
					read_counter += (DISK_BLOCK_SIZE - block_offset);
					length -= (DISK_BLOCK_SIZE - block_offset);
				}
				offset_ptr++;
			}
			size -= DISK_BLOCK_SIZE;
			n_block++;
		}
	}
	free(blocks);
	return read_counter;
}

int fs_write( int inumber, const char *data, int length, int offset ){
	// Mount is a prequisite and inumber must be in range of inodes
	if(!is_mounted || !is_valid_inumber(inumber)) return 0;
	// Don't try to read anything if there is nothing to read or invalid offset
	if(length <= 0 || offset < 0) return 0;

	// Load the inode, size, and ptr info
	struct fs_inode inode;
	inode_load(inumber, &inode);
	int size = inode.size;

	union fs_block block, indirect_block;

	// Get counts of types of free pointers in the inode
	int pointers_used = ceil((double)size / DISK_BLOCK_SIZE);
	bool has_indirect = false;
	if(pointers_used > POINTERS_PER_INODE){
		has_indirect = true;
		disk_read(inode.indirect, indirect_block.data);
	}

	// Start filling in the data overwriting
	int offset_ptr = floor(offset / DISK_BLOCK_SIZE);
	int write_counter = 0, block_offset, block_num;
	while(size - offset > 0){
		if(offset_ptr < POINTERS_PER_INODE){
			block_num = inode.direct[offset_ptr];
		}else{
			block_num = indirect_block.pointers[offset_ptr - POINTERS_PER_INODE];
		}
		//Copy the chunk of data into a block
		block_offset = (offset - (offset_ptr * DISK_BLOCK_SIZE)) % DISK_BLOCK_SIZE;
		if(block_offset + length < DISK_BLOCK_SIZE){
			memcpy(block.data + block_offset, data + write_counter, length);
			write_counter += length;
			size -= length;
			length = 0;
		}else{
			memcpy(block.data + block_offset, data + write_counter, DISK_BLOCK_SIZE - block_offset);
			write_counter += (DISK_BLOCK_SIZE - block_offset);
			size -= (DISK_BLOCK_SIZE - block_offset);
			length -= (DISK_BLOCK_SIZE - block_offset);
		}
		disk_write(block_num, block.data);
		offset_ptr++;
		if(length <= 0){
			return write_counter;
		}
	}

	// Get counts of types of free pointers in the inode
	size = inode.size;
	pointers_used = ceil((double)size / DISK_BLOCK_SIZE);
	int free_direct = 0, free_indirect = POINTERS_PER_BLOCK;
	if(pointers_used <= POINTERS_PER_INODE){
		free_direct = POINTERS_PER_INODE - pointers_used;
	}else if(pointers_used > POINTERS_PER_INODE){
		has_indirect = true;
		free_indirect = POINTERS_PER_BLOCK - (pointers_used - POINTERS_PER_INODE);
	}

	union fs_block super_block;
	disk_read(0, super_block.data);

	// Start filling data into open blocks
	int b;
	for(b = super_block.super.ninodeblocks; b < super_block.super.nblocks; b++){
		if(length <= 0) break;
		else if(!free_block_bm[b]) continue;
		free_block_bm[b] = false;
		// Allocate an indirect block if necessary
		if(free_direct == 0 && !has_indirect){
			inode.indirect = b;
			inode_save(inumber, &inode);
			has_indirect = true;
			continue;
		}
		// Copy the chunk of data into a block
		block_offset = (offset - (offset_ptr * DISK_BLOCK_SIZE)) % DISK_BLOCK_SIZE;
		if(block_offset + length < DISK_BLOCK_SIZE){
			memcpy(block.data + block_offset, data + write_counter, length);
			write_counter += length;
			inode.size += length;
			length = 0;
		}else{
			memcpy(block.data + block_offset, data + write_counter, DISK_BLOCK_SIZE - block_offset);
			write_counter += (DISK_BLOCK_SIZE - block_offset);
			inode.size += (DISK_BLOCK_SIZE - block_offset);
			length -= (DISK_BLOCK_SIZE - block_offset);
		}
		// Put the location of the new data block in a pointer to store in the inode
		if(free_direct > 0){
			inode.direct[(POINTERS_PER_INODE - free_direct) % POINTERS_PER_INODE] = b;
			free_direct--;
		}else if(has_indirect && free_indirect > 0){
			disk_read(inode.indirect, indirect_block.data);
			indirect_block.pointers[(POINTERS_PER_BLOCK - free_indirect) % POINTERS_PER_BLOCK] = b;
			disk_write(inode.indirect, indirect_block.data);
			free_indirect--;
		}
		inode_save(inumber, &inode);
		disk_write(b, block.data);
		offset_ptr++;
	}
	return write_counter;
}
