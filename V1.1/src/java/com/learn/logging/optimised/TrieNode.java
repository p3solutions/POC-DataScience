package com.learn.logging.optimised;

import java.util.ArrayList;
//import java.util.HashSet;

class TrieNode {
	char content;
	boolean isEnd;
	int count;
	ArrayList<TrieNode> childList;

	public TrieNode(char c) {
		childList = new ArrayList<TrieNode>();
		isEnd = false;
		content = c;
		count = 0;
	}

	public TrieNode subNode(char c) {
		if (childList != null)
			for (TrieNode eachChild : childList)
				if (eachChild.content == c)
					return eachChild;
		return null;
	}
}

