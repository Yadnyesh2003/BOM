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
        for r in bom_df.iter_rows(named=True):
            key = (r["Finished_Good"], r["Plant"])
            grouped[key].append({
                "parent": r["Parent"],
                "child": r["Child"],
                "ratio": r["BOM_Ratio_Of_Child"]
            })

        # Build tree per (FG, Plant)
        for key, entries in grouped.items():
            tree = defaultdict(list)
            for e in entries:
                tree[e["parent"]].append(e)
            self.bom_tree_map[key] = tree

    def get_tree(self, fg, plant):
        return self.bom_tree_map.get((fg, plant), {})
