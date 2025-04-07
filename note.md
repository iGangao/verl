1. main_ppo.py -> RayPPOTrainer
2. `ray_trainer.py` -> RayPPOTrainer -> fit
3. ray_trainer.py -> dataloader
4. rl_dataset.py -> RLHFDataset
---(golbal_batch)---
5. ray_trainer.py -> rollout
6. `fsdp_workers.py` -> ActorRolloutRefWorker -> generate_seqences -> vllm_rollout_spmd.py -> vLLMRollout -> `generate_seqences`
7. ray_trainer.py -> compute
8. fsdp_workers.py -> ActorRolloutRefWorker -> compute_log_prob | compute_ref_log_prob
9. fsdp_worker.py -> RewardModelWorker -> `compute_rm_score`
10. ray_trainer.py -> apply_kl_penalty | compute_advantage
11. ray_trainer.py -> update
---(minibatch)----
12. fsdp_workers.py -> update_actor 
13. `do_actor.py` -> DataParallelPPOActor -> update_policy -> compute_loss -> loss.backward




## 