package com.dreamlab.types;

import java.util.Map;
import java.util.UUID;

public class CostModelOutput {
	public Map<UUID, String> perFogLevel2Query;

	public String Level1Query;
//    	1: required map<string,string> perFogLevel2Query;
//	2: required map<string,list<i64>> perFogMbids;
//	3: required map<i64,FogEdge> perMbidLoc;
//
//	4: required string Level1Query;
//	5: required string Level1Fog;
}