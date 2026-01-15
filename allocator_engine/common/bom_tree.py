# from collections import defaultdict

# class BOMTree:
#     def __init__(self, bom_df):
#         self.bom_map_by_fg = defaultdict(list)
#         self.bom_tree_map = {}

#         for r in bom_df.iter_rows(named=True):
#             self.bom_map_by_fg[r["Finished_Good"]].append({
#                 "parent": r["Parent"],
#                 "child": r["Child"],
#                 "ratio": r["BOM_Ratio_Of_Child"]
#             })

#         # Precompute tree for each finished good
#         for fg, entries in self.bom_map_by_fg.items():
#             tree = defaultdict(list)
#             for e in entries:
#                 tree[e["parent"]].append(e)
#             self.bom_tree_map[fg] = tree

#     def get_tree(self, fg):
#         return self.bom_tree_map.get(fg, {})


from collections import defaultdict

class BOMTree:
    def __init__(self, bom_df):
        """
        BOM is uniquely identified by (Finished_Good, Plant)
        """
        self.bom_tree_map = {}

        # Group by FG + Plant
        grouped = defaultdict(list)
        self.parent_index = defaultdict(list)
        for r in bom_df.iter_rows(named=True):
            key = (r["root_parent"], r["plant"])
            grouped[key].append({
                "parent": r["parent"],
                "child": r["child"],
                "ratio": r["comp_qty"]
            })
            # New reverse lookup
            self.parent_index[(r["parent"], r["plant"])].append(r["root_parent"])

        # Build tree per (FG, Plant)
        for key, entries in grouped.items():
            tree = defaultdict(list)
            for e in entries:
                tree[e["parent"]].append(e)
            self.bom_tree_map[key] = tree

    def get_tree(self, fg, plant):
        return self.bom_tree_map.get((fg, plant), {})
    
    def resolve_fg(self, fg, plant):
        """
        Returns:
        - resolved_root_fg
        - bom_tree
        - resolution_type: 'ROOT' | 'SFG'
        """
        # Normal FG case
        if (fg, plant) in self.bom_tree_map:
            return fg, self.bom_tree_map[(fg, plant)], "ROOT"
        # SFG fallback
        candidates = self.parent_index.get((fg, plant), [])
        if candidates:
            root_fg = candidates[0]  # deterministic first match
            return root_fg, self.bom_tree_map[(root_fg, plant)], "SFG"
        # Not found
        return None, None, "NOT_FOUND"

