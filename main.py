from referrals import managers

man = managers.ReferralsAnalysis()

user_stages, user_interactions = man.run()

print(user_stages.columns)
print()
print(user_interactions.columns)
print()
print('Done')